package com.redis.smartcache.cli.commands;

import com.redis.smartcache.cli.RedisServiceImpl;
import com.redis.smartcache.cli.components.TableSelector;
import com.redis.smartcache.cli.structures.QueryInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.component.support.SelectorItem;
import org.springframework.shell.standard.AbstractShellComponent;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@ShellComponent
public class Commands extends AbstractShellComponent {
    @Autowired
    RedisServiceImpl client;

    @ShellMethod(key = "ping", value = "ping")
    String ping(){
        return client.ping();
    }

    @ShellMethod(key = "list-queries", value = "Get the table of queries", group = "Components")
    public String queryTable(){
        List<SelectorItem<QueryInfo>> queries = new ArrayList<>();

        for (QueryInfo q : client.getQueries("smartcache")){
            queries.add(SelectorItem.of(q.getQueryId(),q));
        }

        TableSelector<QueryInfo, SelectorItem<QueryInfo>> component = new TableSelector<>(getTerminal(),
                queries, "queries", null, QueryInfo.getHeaderRow((getTerminal().getWidth()-10)/8));
        component.setResourceLoader(getResourceLoader());
        component.setTemplateExecutor(getTemplateExecutor());
        TableSelector.SingleItemSelectorContext<QueryInfo, SelectorItem<QueryInfo>> context = component
                .run(TableSelector.SingleItemSelectorContext.empty());
        QueryInfo result = context.getResultItem().flatMap(si -> Optional.ofNullable(si.getItem())).get();
        return "Got value " + result.toString();
    }
}
