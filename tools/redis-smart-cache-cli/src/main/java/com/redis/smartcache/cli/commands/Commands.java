package com.redis.smartcache.cli.commands;

import com.redis.smartcache.cli.RedisServiceImpl;
import com.redis.smartcache.cli.components.TableSelector;
import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.core.Config.RuleConfig;
import com.redis.smartcache.core.rules.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.component.StringInput;
import org.springframework.shell.component.support.SelectorItem;
import org.springframework.shell.standard.AbstractShellComponent;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import io.airlift.units.Duration;
import java.util.*;

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
        List<RuleConfig> rules = client.getRules();

        Map<Duration, RuleConfig> pendingRules = new HashMap<>();

        List<SelectorItem<QueryInfo>> queries = new ArrayList<>();

        for (QueryInfo q : client.getQueries("smartcache")){
            queries.add(SelectorItem.of(q.getQueryId(),q));
        }

        while(true){
            TableSelector<QueryInfo, SelectorItem<QueryInfo>> component = new TableSelector<>(getTerminal(),
                    queries, "queries", null, QueryInfo.getHeaderRow((getTerminal().getWidth()-10)/8), true);
            component.setResourceLoader(getResourceLoader());
            component.setTemplateExecutor(getTemplateExecutor());
            TableSelector.SingleItemSelectorContext<QueryInfo, SelectorItem<QueryInfo>> context = component
                    .run(TableSelector.SingleItemSelectorContext.empty());

            Optional<SelectorItem<QueryInfo>> resOpt = context.getResultItem();

            if (component.isConfirmMode()){
                Set<String> validResponses = new HashSet<>(Arrays.asList("y","Y","n","N"));
                Optional<Boolean> confirmed = Optional.empty();
                while(!confirmed.isPresent()){

                    String prompt = "Confirm pending updates y/n";
                    StringInput stringInputComponent = new StringInput(getTerminal(), prompt,"n");
                    stringInputComponent.setResourceLoader(getResourceLoader());
                    stringInputComponent.setTemplateExecutor(getTemplateExecutor());
                    StringInput.StringInputContext stringInputContext = stringInputComponent.run(StringInput.StringInputContext.empty());
                    String confirmationInput = stringInputContext.getResultValue();
                    if(validResponses.contains(confirmationInput)){
                        confirmed = Optional.of(confirmationInput.equalsIgnoreCase("y"));
                    }

                    if(confirmed.get()){
                        for(RuleConfig rule : pendingRules.values()){
                            rules.add(0, rule);
                        }
                    }
                    else{
                        component.setConfirmMode(false);
                    }
                }
            }
            else if (resOpt.isPresent()){
                QueryInfo result = resOpt.get().getItem();

                String info = result.toFormattedString(getTerminal().getWidth());
                Optional<Duration> duration = Optional.empty();

                String prompt = String.format("%s%nEnter TTL:", info);

                while (!duration.isPresent()){
                    StringInput stringInputComponent = new StringInput(getTerminal(),prompt,"30m");
                    stringInputComponent.setResourceLoader(getResourceLoader());
                    stringInputComponent.setTemplateExecutor(getTemplateExecutor());
                    StringInput.StringInputContext stringInputContext = stringInputComponent.run(StringInput.StringInputContext.empty());
                    try {
                        duration = Optional.of(Duration.valueOf(stringInputContext.getResultValue()));
                    } catch (IllegalArgumentException ex){
                        prompt = String.format("%s%nPrevious Input was Invalid%nEnter TTL", info);
                    }
                }

                RuleConfig rule;
                if(pendingRules.containsKey(duration.get())){
                    pendingRules.get(duration.get()).getQueryIds().add(result.getQueryId());
                    rule = pendingRules.get(duration.get());
                }else{
                    rule = new RuleConfig.Builder().queryIds(result.getQueryId()).ttl(duration.get()).build();
                    pendingRules.put(duration.get(),rule);
                }
                queries.get(context.getCursorRow()).getItem().setPendingRule(rule);
//                return String.format("Selected query id: %s index: %d, ttl: %s", result.getQueryId(), context.getCursorRow(), stringInputContext.getResultValue());
            }
            else{
                break;
            }
        }

        return "";
    }
}
