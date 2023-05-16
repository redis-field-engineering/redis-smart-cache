package com.redis.smartcache.cli.components;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;
import org.springframework.shell.component.StringInput;
import org.springframework.util.StringUtils;

import static org.jline.keymap.KeyMap.del;
import static org.jline.keymap.KeyMap.key;

public class StringInputExtension extends StringInput {

    public boolean isEscapeMode() {
        return isEscapeMode;
    }

    private boolean isEscapeMode = false;
    private final String OPERATION_ESCAPE = "ESCAPE";

    public StringInputExtension(Terminal terminal, String name, String defaultValue) {
        super(terminal, name, defaultValue);
    }

    @Override
    protected void bindKeyMap(KeyMap<String> keyMap) {
        keyMap.bind(OPERATION_ESCAPE,"\033");
        keyMap.bind(OPERATION_EXIT, "\r");
        keyMap.bind(OPERATION_BACKSPACE, del(), key(getTerminal(), InfoCmp.Capability.key_backspace));
        // skip 127 - DEL
        for (char i = 32; i < KeyMap.KEYMAP_LENGTH - 1; i++) {
            keyMap.bind(OPERATION_CHAR, Character.toString(i));
        }
    }

    @Override
    protected boolean read(BindingReader bindingReader, KeyMap<String> keyMap, StringInputContext context) {
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
                break;
            case OPERATION_BACKSPACE:
                input = context.getInput();
                if (StringUtils.hasLength(input)) {
                    input = input.length() > 1 ? input.substring(0, input.length() - 1) : null;
                }
                context.setInput(input);
                break;
            case OPERATION_EXIT:
                if (StringUtils.hasText(context.getInput())) {
                    context.setResultValue(context.getInput());
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
