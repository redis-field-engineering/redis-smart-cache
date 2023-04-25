package com.redis.smartcache.cli.components;

import com.redis.smartcache.cli.structures.TableInfoItem;
import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.InfoCmp;
import org.springframework.shell.component.context.ComponentContext;
import org.springframework.shell.component.support.*;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.jline.keymap.KeyMap.*;
import static org.jline.keymap.KeyMap.key;

public class TableSelector<T extends TableInfoItem, I extends Nameable & Matchable & Enableable & Selectable & Itemable<T>>
        extends AbstractSelectorComponent<T, TableSelector.SingleItemSelectorContext<T, I>, I> {

    private SingleItemSelectorContext<T, I> currentContext;

    private String header;

    public TableSelector(Terminal terminal, List<I> items, String name, Comparator<I> comparator, String header) {
        super(terminal, name, items, true, comparator);
        this.header = header;
        setRenderer(new DefaultRenderer());
        setTemplateLocation("classpath:table-selector.stg");
    }

    @Override
    public SingleItemSelectorContext<T, I> getThisContext(ComponentContext<?> context) {
        if (context != null && currentContext == context) {
            return currentContext;
        }
        currentContext = TableSelector.SingleItemSelectorContext.empty(getItemMapper());
        currentContext.setName(name);
        currentContext.setHeader(header);
        currentContext.setWidth(getTerminal().getWidth());
        if (currentContext.getItems() == null) {
            currentContext.setItems(getItems());
        }
        context.stream().forEach(e -> {
            currentContext.put(e.getKey(), e.getValue());
        });
        return currentContext;
    }

    @Override
    protected SingleItemSelectorContext<T, I> runInternal(SingleItemSelectorContext<T, I> context) {
        super.runInternal(context);
        // if there's no tty don't try to loop as it would then cause user interaction
        if (hasTty()) {
            loop(context);
        }
        return context;
    }

    @Override
    protected void bindKeyMap(KeyMap<String> keyMap) {
        keyMap.bind(OPERATION_SELECT, " ");
        keyMap.bind(OPERATION_DOWN, ctrl('E'), key(getTerminal(), InfoCmp.Capability.key_down));
        keyMap.bind(OPERATION_UP, ctrl('Y'), key(getTerminal(), InfoCmp.Capability.key_up));
        keyMap.bind(OPERATION_EXIT, "\r");
        keyMap.bind(OPERATION_BACKSPACE, del(), key(getTerminal(), InfoCmp.Capability.key_backspace));
    }

//    @Override
//    protected boolean read(BindingReader bindingReader, KeyMap<String> keyMap, TableSelector.SingleItemSelectorContext<T, I>  context){
//
//        if (stale) {
//            start.set(0);
//            pos.set(0);
//            stale = false;
//        }
//        TableSelector.SingleItemSelectorContext<T, I> thisContext = getThisContext(context);
//        ItemStateViewProjection buildItemStateView = buildItemStateView(start.get(), thisContext);
//        List<ItemState<I>> itemStateView = buildItemStateView.items;
//        String operation = bindingReader.readBinding(keyMap);
//        log.debug("Binding read result {}", operation);
//        if (operation == null) {
//            return true;
//        }
//        String input;
//        switch (operation) {
//            case OPERATION_SELECT:
//                if (!exitSelects) {
//                    itemStateView.forEach(i -> {
//                        if (i.index == start.get() + pos.get() && i.enabled) {
//                            i.selected = !i.selected;
//                        }
//                    });
//                }
//                break;
//            case OPERATION_DOWN:
//                if (start.get() + pos.get() + 1 < itemStateView.size()) {
//                    pos.incrementAndGet();
//                }
//                else if (start.get() + pos.get() + 1 >= buildItemStateView.total) {
//                    start.set(0);
//                    pos.set(0);
//                }
//                else {
//                    start.incrementAndGet();
//                }
//                break;
//            case OPERATION_UP:
//                if (start.get() > 0 && pos.get() == 0) {
//                    start.decrementAndGet();
//                }
//                else if (start.get() + pos.get() >= itemStateView.size()) {
//                    pos.decrementAndGet();
//                }
//                else if (start.get() + pos.get() <= 0) {
//                    start.set(buildItemStateView.total - Math.min(maxItems, itemStateView.size()));
//                    pos.set(itemStateView.size() - 1);
//                }
//                else {
//                    pos.decrementAndGet();
//                }
//                break;
//            case OPERATION_BACKSPACE:
//                input = thisContext.getInput();
//                if (StringUtils.hasLength(input)) {
//                    input = input.length() > 1 ? input.substring(0, input.length() - 1) : null;
//                }
//                thisContext.setInput(input);
//                break;
//            case OPERATION_EXIT:
//                if (exitSelects) {
//                    if (itemStateView.size() == 0) {
//                        // filter shows nothing, prevent exit
//                        break;
//                    }
//                    itemStateView.forEach(i -> {
//                        if (i.index == start.get() + pos.get()) {
//                            i.selected = !i.selected;
//                        }
//                    });
//                }
//                List<I> values = thisContext.getItemStates().stream()
//                        .filter(i -> i.selected)
//                        .map(i -> i.item)
//                        .collect(Collectors.toList());
//                thisContext.setResultItems(values);
//                return true;
//            default:
//                break;
//        }
//        thisContext.setCursorRow(start.get() + pos.get());
//        buildItemStateView = buildItemStateView(start.get(), thisContext);
//        thisContext.setItemStateView(buildItemStateView.items);
//        return false;
//    }
//

    /**
     * Context {@link TableSelector}.
     */
    public interface SingleItemSelectorContext<T extends TableInfoItem, I extends Nameable & Matchable & Itemable<T>>
            extends SelectorComponentContext<T, I, SingleItemSelectorContext<T, I>> {

        /**
         * Gets a result item.
         *
         * @return a result item
         */
        Optional<I> getResultItem();

        /**
         * Gets a value.
         *
         * @return a value
         */
        Optional<String> getValue();

        void setHeader(String header);

        void setWidth(int width);

        /**
         * Creates an empty {@link SingleItemSelectorContext}.
         *
         * @return empty context
         */
        static <C extends TableInfoItem, I extends Nameable & Matchable & Itemable<C>> SingleItemSelectorContext<C, I> empty() {
            return new TableSelector.DefaultSingleItemSelectorContext<>();
        }

        /**
         * Creates a {@link SingleItemSelectorContext}.
         *
         * @return context
         */
        static <C extends TableInfoItem, I extends Nameable & Matchable & Itemable<C>> SingleItemSelectorContext<C, I> empty(Function<C, String> itemMapper) {
            return new TableSelector.DefaultSingleItemSelectorContext<>(itemMapper);
        }
    }

    private static class DefaultSingleItemSelectorContext<T extends TableInfoItem, I extends Nameable & Matchable & Itemable<T>> extends
            BaseSelectorComponentContext<T, I, SingleItemSelectorContext<T, I>> implements SingleItemSelectorContext<T, I> {

        private String header;
        private int width;

        private int getColWidth(){
            return (width-10)/8;
        }

        public void setWidth(int width){
            this.width = width;

        }

        public void setHeader(String header){
            this.header = header;
        }

        private Function<T, String> itemMapper = item -> item.toRowString(width);

        DefaultSingleItemSelectorContext() {
        }

        DefaultSingleItemSelectorContext(Function<T, String> itemMapper) {
            this.itemMapper = itemMapper;
        }

        @Override
        public Optional<I> getResultItem() {
            if (getResultItems() == null) {
                return Optional.empty();
            }
            return getResultItems().stream().findFirst();
        }

        @Override
        public Optional<String> getValue() {
            return getResultItem().map(item -> itemMapper.apply(item.getItem()));
        }

        @Override
        public Map<String, Object> toTemplateModel() {
            Map<String, Object> attributes = super.toTemplateModel();
            attributes.put("header", header);
//            getValue().ifPresent(value -> {
//                attributes.put("value", value);
//            });©

            List<Map<String,Object>> rows = new ArrayList<>();
            for (int i = 0; i<getItems().size();i++){
                Map<String,Object> map = new HashMap<>();
                map.put("name", getItems().get(i).getItem().toRowString(getColWidth()));
                map.put("selected", getCursorRow().intValue() == i);
                rows.add(map);
            }

            attributes.put("rows", rows);
            // finally wrap it into 'model' as that's what
            // we expect in stg template.
            Map<String, Object> model = new HashMap<>();
            model.put("model", attributes);
            return model;
        }

        @Override
        public String toString() {
            return "DefaultSingleItemSelectorContext [super=" + super.toString() + "]";
        }
    }

    private class DefaultRenderer implements Function<SingleItemSelectorContext<T, I>, List<AttributedString>> {

        @Override
        public List<AttributedString> apply(SingleItemSelectorContext<T, I> context) {
            return renderTemplateResource(context.toTemplateModel());
        }
    }
}

