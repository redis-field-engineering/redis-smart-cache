package com.redis.smartcache.cli.components;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;
import org.springframework.shell.component.context.BaseComponentContext;
import org.springframework.shell.component.context.ComponentContext;
import org.springframework.shell.component.support.*;
import org.springframework.util.ObjectUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.jline.keymap.KeyMap.ctrl;
import static org.jline.keymap.KeyMap.key;

public abstract class AbstractTableSelectorComponent<T, C extends AbstractTableSelectorComponent.SelectorComponentContext<T, I, C>, I extends Nameable & Matchable & Enableable & Selectable & Itemable<T>>
        extends AbstractComponent<C> {

    protected final String name;
    private final List<I> items;
    private Comparator<I> comparator = (o1, o2) -> 0;
    private final boolean exitSelects;
    private final int maxItems = 5;
    private boolean stale = false;
    private final AtomicInteger start = new AtomicInteger(0);
    private final AtomicInteger pos = new AtomicInteger(0);
    private I defaultExpose;
    private boolean expose = false;
    private boolean confirmMode = false;
    private boolean newMode = false;
    private boolean deleteMode = false;

    public boolean isEscapeMode() {
        return escapeMode;
    }

    private boolean escapeMode = false;

    public static final String OPERATION_ESCAPE = "ESCAPE";
    public static final String OPERATION_CONFIRM = "CONFIRM";
    public static final String OPERATION_NEW = "NEW";
    public static final String OPERATION_DELETE = "DELETE";
    public AbstractTableSelectorComponent(Terminal terminal, String name, List<I> items, boolean exitSelects,
                                          Comparator<I> comparator) {
        super(terminal);
        this.name = name;
        this.items = items;
        this.exitSelects = exitSelects;
        if (comparator != null) {
            this.comparator = comparator;
        }
    }

    /**
     * Gets items.
     *
     * @return a list of items
     */
    protected List<I> getItems() {
        return items;
    }

    @Override
    protected void bindKeyMap(KeyMap<String> keyMap) {
        keyMap.setAmbiguousTimeout(1);
        keyMap.bind(OPERATION_DOWN, ctrl('E'), key(getTerminal(), InfoCmp.Capability.key_down));
        keyMap.bind(OPERATION_UP, ctrl('Y'), key(getTerminal(), InfoCmp.Capability.key_up));
        keyMap.bind(OPERATION_EXIT, "\r");
        keyMap.bind(OPERATION_ESCAPE,"\033");
        keyMap.bind(OPERATION_CONFIRM, Character.toString('c'));
        keyMap.bind(OPERATION_NEW, Character.toString('n'));
        keyMap.bind(OPERATION_DELETE, Character.toString('d'));

    }

    @Override
    protected C runInternal(C context) {
        C thisContext = getThisContext(context);
        initialExpose(thisContext);
        ItemStateViewProjection buildItemStateView = buildItemStateView(start.get(), thisContext);
        List<ItemState<I>> itemStateView = buildItemStateView.items;
        thisContext.setItemStateView(itemStateView);
        if(context != null && context.getCursorRow() != 0){
            pos.set(context.getCursorRow()-start.get());
        }

        thisContext.setCursorRow(start.get() + pos.get());

        return thisContext;
    }

    @Override
    protected boolean read(BindingReader bindingReader, KeyMap<String> keyMap, C context) {
        String operation = bindingReader.readBinding(keyMap);
        if(Objects.equals(operation, OPERATION_ESCAPE)){
            escapeMode = true;
            return true;
        }

        if (stale) {
            start.set(0);
            pos.set(0);
            stale = false;
        }

        C thisContext = getThisContext(context);
        ItemStateViewProjection buildItemStateView = buildItemStateView(start.get(), thisContext);
        List<ItemState<I>> itemStateView = buildItemStateView.items;

        if (operation == null) {
            return true;
        }

        switch (operation) {
            case OPERATION_DOWN:
                if (start.get() + pos.get() + 1 < itemStateView.size()) {
                    pos.incrementAndGet();
                }
                else if (start.get() + pos.get() + 1 >= buildItemStateView.total) {
                    start.set(0);
                    pos.set(0);
                }
                else {
                    start.incrementAndGet();
                }
                break;
            case OPERATION_UP:
                if (start.get() > 0 && pos.get() == 0) {
                    start.decrementAndGet();
                }
                else if (start.get() + pos.get() >= itemStateView.size()) {
                    pos.decrementAndGet();
                }
                else if (start.get() + pos.get() <= 0) {
                    start.set(buildItemStateView.total - Math.min(maxItems, itemStateView.size()));
                    pos.set(itemStateView.size() - 1);
                }
                else {
                    pos.decrementAndGet();
                }
                break;
            case OPERATION_EXIT:
                if (exitSelects) {
                    if (itemStateView.size() == 0) {
                        // filter shows nothing, prevent exit
                        break;
                    }
                    itemStateView.forEach(i -> {
                        if (i.index == start.get() + pos.get()) {
                            i.selected = !i.selected;
                        }
                    });
                }
                List<I> values = thisContext.getItemStates().stream()
                        .filter(i -> i.selected)
                        .map(i -> i.item)
                        .collect(Collectors.toList());
                thisContext.setResultItems(values);
                return true;
            case OPERATION_CONFIRM:
                confirmMode = true;
                return true;
            case OPERATION_NEW:
                newMode = true;
                return true;
            case OPERATION_DELETE:
                deleteMode = true;
                return true;
            default:
                break;
        }

        thisContext.setCursorRow(start.get() + pos.get());
        buildItemStateView = buildItemStateView(start.get(), thisContext);
        thisContext.setItemStateView(buildItemStateView.items);
        return false;
    }

    private void initialExpose(C context) {
        if (!expose) {
            return;
        }
        expose = false;
        List<ItemState<I>> itemStates = context.getItemStates();
        if (itemStates == null) {
            AtomicInteger index = new AtomicInteger(0);
            itemStates = context.getItems().stream()
                    .sorted(comparator)
                    .map(item -> ItemState.of(item, item.getName(), index.getAndIncrement(), item.isEnabled(), item.isSelected()))
                    .collect(Collectors.toList());
        }
        for (int i = 0; i < itemStates.size(); i++) {
            if (ObjectUtils.nullSafeEquals(itemStates.get(i).getName(), defaultExpose.getName())) {
                if (i < maxItems) {
                    this.pos.set(i);
                }
                else {
                    this.pos.set(maxItems - 1);
                    this.start.set(i - maxItems + 1);
                }
                break;
            }
        }
    }

    private ItemStateViewProjection buildItemStateView(int skip, SelectorComponentContext<T, I, ?> context) {
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
                .filter(i -> i.matches(context.getInput()))
                .peek(i -> i.index = reindex.getAndIncrement())
                .collect(Collectors.toList());
        List<ItemState<I>> items = filtered.stream()
                .skip(skip)
                .limit(maxItems)
                .collect(Collectors.toList());
        return new ItemStateViewProjection(items, filtered.size());
    }

    public boolean isConfirmMode() {
        return confirmMode;
    }
    public boolean isNewMode() { return newMode; }
    public boolean isDeleteMode(){ return deleteMode; }

    public void setConfirmMode(boolean confirmMode) {
        this.confirmMode = confirmMode;
    }
    public void setNewMode(boolean newMode) { this.newMode = newMode; }
    public void setDeleteMode(boolean deleteMode){ this.deleteMode=deleteMode; }

    class ItemStateViewProjection {
        List<ItemState<I>> items;
        int total;
        ItemStateViewProjection(List<ItemState<I>> items, int total) {
            this.items = items;
            this.total = total;
        }
    }

    /**
     * Context interface on a selector component sharing content.
     */
    public interface SelectorComponentContext<T, I extends Nameable & Matchable & Itemable<T>, C extends SelectorComponentContext<T, I, C>>
            extends ComponentContext<C> {

        /**
         * Sets a name
         *
         * @param name the name
         */
        void setName(String name);

        /**
         * Gets an input.
         *
         * @return an input
         */
        String getInput();

        /**
         * Gets an item states
         *
         * @return an item states
         */
        List<ItemState<I>> getItemStates();

        /**
         * Sets an item states.
         *
         * @param itemStateView the input state
         */
        void setItemStates(List<ItemState<I>> itemStateView);

        /**
         * Sets an item state view
         *
         * @param itemStateView the item state view
         */
        void setItemStateView(List<ItemState<I>> itemStateView);

        /**
         * Gets a cursor row.
         *
         * @return a cursor row.
         */
        Integer getCursorRow();

        /**
         * Sets a cursor row.
         *
         * @param cursorRow the cursor row
         */
        void setCursorRow(Integer cursorRow);

        /**
         * Gets an items.
         *
         * @return an items
         */
        List<I> getItems();

        /**
         * Sets an items.
         *
         * @param items the items
         */
        void setItems(List<I> items);

        /**
         * Gets a result items.
         *
         * @return a result items
         */
        List<I> getResultItems();

        /**
         * Sets a result items.
         *
         * @param items the result items
         */
        void setResultItems(List<I> items);

    }

    /**
     * Base implementation of a {@link org.springframework.shell.component.support.AbstractSelectorComponent.SelectorComponentContext}.
     */
    protected static class BaseSelectorComponentContext<T, I extends Nameable & Matchable & Itemable<T>, C extends SelectorComponentContext<T, I, C>>
            extends BaseComponentContext<C> implements SelectorComponentContext<T, I, C> {

        private String name;
        private String input;
        private List<ItemState<I>> itemStates;
        private List<ItemState<I>> itemStateView;
        private Integer cursorRow;
        private List<I> items;
        private List<I> resultItems;

        private String getName() {
            return name;
        }

        @Override
        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String getInput() {
            return input;
        }

        @Override
        public List<ItemState<I>> getItemStates() {
            return itemStates;
        }

        @Override
        public void setItemStates(List<ItemState<I>> itemStates) {
            this.itemStates = itemStates;
        }

        private List<ItemState<I>> getItemStateView() {
            return itemStateView;
        }

        @Override
        public void setItemStateView(List<ItemState<I>> itemStateView) {
            this.itemStateView = itemStateView;
        }

        private boolean isResult() {
            return resultItems != null;
        }

        @Override
        public Integer getCursorRow() {
            return cursorRow;
        }

        @Override
        public java.util.Map<String,Object> toTemplateModel() {
            Map<String, Object> attributes = super.toTemplateModel();
            attributes.put("name", getName());
            attributes.put("input", getInput());
            attributes.put("itemStates", getItemStates());
            attributes.put("itemStateView", getItemStateView());
            attributes.put("isResult", isResult());
            attributes.put("cursorRow", getCursorRow());
            return attributes;
        }

        public void setCursorRow(Integer cursorRow) {
            this.cursorRow = cursorRow;
        }

        @Override
        public List<I> getItems() {
            return items;
        }

        @Override
        public void setItems(List<I> items) {
            this.items = items;
        }

        @Override
        public List<I> getResultItems() {
            return resultItems;
        }

        @Override
        public void setResultItems(List<I> resultItems) {
            this.resultItems = resultItems;
        }

        @Override
        public String toString() {
            return "DefaultSelectorComponentContext [cursorRow=" + cursorRow + "]";
        }

    }

    /**
     * Class keeping item state.
     */
    public static class ItemState<I extends Matchable> implements Matchable {
        I item;
        String name;
        boolean selected;
        boolean enabled;
        int index;

        ItemState(I item, String name, int index, boolean enabled, boolean selected) {
            this.item = item;
            this.name = name;
            this.index = index;
            this.enabled = enabled;
            this.selected = selected;
        }

        public boolean matches(String match) {
            return item.matches(match);
        }

        public String getName() {
            return name;
        }

        static <I extends Matchable> ItemState<I> of(I item, String name, int index, boolean enabled, boolean selected) {
            return new ItemState<>(item, name, index, enabled, selected);
        }
    }

}

