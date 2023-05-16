package com.redis.smartcache.cli.components;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.springframework.shell.component.ConfirmationInput;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.function.Function;

public class ConfirmationInputExtension extends ConfirmationInput {

    private final String OPERATION_ESCAPE = "ESCAPE";

    public boolean isEscapeMode() {
        return isEscapeMode;
    }

    private boolean isEscapeMode = false;
    public ConfirmationInputExtension(Terminal terminal, String name, boolean defaultValue) {
        super(terminal, name, defaultValue);
        setTemplateLocation("classpath:confirmation-input.stg");
    }

    public ConfirmationInputExtension(Terminal terminal, String name, boolean defaultValue, Function<ConfirmationInputContext, List<AttributedString>> renderer) {
        super(terminal, name, defaultValue, renderer);
    }

    @Override
    protected void bindKeyMap(KeyMap<String> keyMap) {
        keyMap.bind(OPERATION_ESCAPE, "\033");
        super.bindKeyMap(keyMap);
    }

    private void checkInput(String input, ConfirmationInputContext context) {
        if (!StringUtils.hasText(input)) {
            context.setMessage(null);
            return;
        }
        Boolean yesno =  parseBoolean(input);
        if (yesno == null) {
            String msg = String.format("Sorry, your input is invalid: '%s', try again", input);
            context.setMessage(msg, TextComponentContext.MessageLevel.ERROR);
        }
        else {
            context.setMessage(null);
        }
    }

    private Boolean parseBoolean(String input) {
        if (!StringUtils.hasText(input)) {
            return null;
        }
        input = input.trim().toLowerCase();
        switch (input) {
            case "y":
            case "yes":
            case "1":
                return true;
            case "n":
            case "no":
            case "0":
                return false;
            default:
                return null;
        }
    }

    @Override
    protected boolean read(BindingReader bindingReader, KeyMap<String> keyMap, ConfirmationInputContext context) {
        String operation = bindingReader.readBinding(keyMap);
        if (operation == null) {
            return true;
        }
        String input;
        switch (operation) {
            case OPERATION_CHAR:
                String lastBinding = bindingReader.getLastBinding();
                input = context.getInput();
                if (input == null) {
                    input = lastBinding;
                }
                else {
                    input = input + lastBinding;
                }
                context.setInput(input);
                checkInput(input, context);
                break;
            case OPERATION_BACKSPACE:
                input = context.getInput();
                if (StringUtils.hasLength(input)) {
                    input = input.length() > 1 ? input.substring(0, input.length() - 1) : null;
                }
                context.setInput(input);
                checkInput(input, context);
                break;
            case OPERATION_EXIT:
                if (StringUtils.hasText(context.getInput())) {
                    context.setResultValue(parseBoolean(context.getInput()));
                }
                else if (context.getDefaultValue() != null) {
                    context.setResultValue(context.getDefaultValue());
                }
                return true;
            case OPERATION_ESCAPE:
                isEscapeMode = true;
                return true;
            default:
                break;
        }
        return false;
    }
}
