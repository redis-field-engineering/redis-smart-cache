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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.jline.keymap.KeyMap.*;
import static org.jline.keymap.KeyMap.key;

public class TableSelector<T extends TableInfoItem, I extends Nameable & Matchable & Enableable & Selectable & Itemable<T>>
        extends AbstractTableSelectorComponent<T, TableSelector.SingleItemSelectorContext<T, I>, I> {

    private SingleItemSelectorContext<T, I> currentContext;

    private boolean exitSelects;
    private boolean stale = false;
    private String header;
    private AtomicInteger start = new AtomicInteger(0);
    private AtomicInteger pos = new AtomicInteger(0);
    private Comparator<I> comparator = (o1, o2) -> 0;
    private int maxItems = 5;

    public TableSelector(Terminal terminal, List<I> items, String name, Comparator<I> comparator, String header, boolean exitSelects) {
        super(terminal, name, items, exitSelects, comparator);
        this.header = header;
        setRenderer(new DefaultRenderer());
        setTemplateLocation("classpath:table-selector.stg");
        if (comparator != null){
            this.comparator = comparator;
        }
        this.exitSelects = exitSelects;
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

    private AbstractTableSelectorComponent.ItemStateViewProjection buildItemStateView(int skip, SelectorComponentContext<T, I, ?> context) {
        List<ItemState<I>> itemStates = context.getItemStates();
        if (itemStates == null) {
            AtomicInteger index = new AtomicInteger(0);
            itemStates = context.getItems().stream()
                    .sorted(comparator)
                    .map(item -> ItemState.of(item, item.getName(), index.getAndIncrement(), item.isEnabled(), item.isSelected()))
                    .collect(Collectors.toList());
            context.setItemStates(itemStates);
        }
        AtomicInteger reindex = new AtomicInteger(0);
        List<ItemState<I>> filtered = itemStates.stream()
                .filter(i -> {
                    return i.matches(context.getInput());
                })
                .map(i -> {
                    i.index = reindex.getAndIncrement();
                    return i;
                })
                .collect(Collectors.toList());
        List<ItemState<I>> items = filtered.stream()
                .skip(skip)
                .limit(maxItems)
                .collect(Collectors.toList());
        return new AbstractTableSelectorComponent.ItemStateViewProjection(items, filtered.size());
    }

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
            if (!getResultItem().isPresent()){
                return Optional.empty();
            }

            return getResultItem().map(item -> itemMapper.apply(item.getItem()));
        }

        @Override
        public Map<String, Object> toTemplateModel() {
            Map<String, Object> attributes = super.toTemplateModel();
            attributes.put("header", header);

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

