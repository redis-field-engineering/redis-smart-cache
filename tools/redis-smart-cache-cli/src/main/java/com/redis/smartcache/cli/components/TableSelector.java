package com.redis.smartcache.cli.components;

import com.redis.smartcache.cli.structures.RowInfo;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.springframework.shell.component.context.ComponentContext;
import org.springframework.shell.component.support.*;

import java.util.*;
import java.util.function.Function;

public class TableSelector<T extends RowInfo, I extends Nameable & Matchable & Enableable & Selectable & Itemable<T>>
        extends AbstractTableSelectorComponent<T, TableSelector.SingleItemSelectorContext<T, I>, I> {

    private SingleItemSelectorContext<T, I> currentContext;

    private final String header;
    private final int numColumns;
    private final String instructions;

    public TableSelector(Terminal terminal, List<I> items, String name, Comparator<I> comparator, String header, boolean exitSelects, int numColumns, String instructions) {
        super(terminal, name, items, exitSelects, comparator);
        this.header = header;
        setRenderer(new DefaultRenderer());
        setTemplateLocation("classpath:table-selector.stg");
        this.numColumns = numColumns;
        this.instructions = instructions;
    }

    @Override
    public SingleItemSelectorContext<T, I> getThisContext(ComponentContext<?> context) {
        if (context != null && currentContext == context) {
            return currentContext;
        }

        int cursorRow = 0;
        if(context !=null){
            if(context instanceof SingleItemSelectorContext){
                Integer cr = ((SingleItemSelectorContext<?, ?>)context).getCursorRow();
                if(cr != null){
                    cursorRow = cr;
                }
            }
        }

        currentContext = TableSelector.SingleItemSelectorContext.empty(numColumns, instructions);
        currentContext.setName(name);
        currentContext.setCursorRow(cursorRow);
        currentContext.setHeader(header);
        currentContext.setWidth(getTerminal().getWidth());
        if (currentContext.getItems() == null) {
            currentContext.setItems(getItems());
        }
        if (context != null) {
            context.stream().forEach(e -> currentContext.put(e.getKey(), e.getValue()));
        }
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

    /**
     * Context {@link TableSelector}.
     */
    public interface SingleItemSelectorContext<T extends RowInfo, I extends Nameable & Matchable & Itemable<T>>
            extends SelectorComponentContext<T, I, SingleItemSelectorContext<T, I>> {

        /**
         * Gets a result item.
         *
         * @return a result item
         */
        Optional<I> getResultItem();

        void setHeader(String header);

        void setWidth(int width);

        /**
         * Creates an empty {@link SingleItemSelectorContext}.
         *
         * @return empty context
         */
        static <C extends RowInfo, I extends Nameable & Matchable & Itemable<C>> SingleItemSelectorContext<C, I> empty(int numColumns, String instructions) {
            return new TableSelector.DefaultSingleItemSelectorContext<>(numColumns,instructions);
        }
    }

    private static class DefaultSingleItemSelectorContext<T extends RowInfo, I extends Nameable & Matchable & Itemable<T>> extends
            BaseSelectorComponentContext<T, I, SingleItemSelectorContext<T, I>> implements SingleItemSelectorContext<T, I> {

        private final String instructions;
        private final int numColumns;
        private String header;
        private int width;

        private int getColWidth(){
            return (width-10)/numColumns;
        }

        public void setWidth(int width){
            this.width = width;

        }

        public void setHeader(String header){
            this.header = header;
        }

        DefaultSingleItemSelectorContext(int numColumns, String instructions) {
            this.numColumns = numColumns;
            this.instructions = instructions;
        }

        @Override
        public Optional<I> getResultItem() {
            if (getResultItems() == null) {
                return Optional.empty();
            }
            return getResultItems().stream().findFirst();
        }

        @Override
        public Map<String, Object> toTemplateModel() {
            Map<String, Object> attributes = super.toTemplateModel();
            attributes.put("header", header);
            attributes.put("instructions", instructions);

            List<Map<String,Object>> rows = new ArrayList<>();
            for (int i = 0; i<getItems().size();i++){
                Map<String,Object> map = new HashMap<>();
                map.put("name", getItems().get(i).getItem().toRowString(getColWidth()));
                map.put("selected", getCursorRow() == i);
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

