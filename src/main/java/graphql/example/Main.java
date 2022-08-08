package graphql.example;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.com.google.common.collect.Lists;
import graphql.execution.instrumentation.ChainedInstrumentation;
import graphql.execution.instrumentation.Instrumentation;
import graphql.execution.instrumentation.tracing.TracingInstrumentation;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderFactory;
import org.dataloader.DataLoaderRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

public class Main {
    private static final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();

    public static void main(String[] args) throws Throwable {
        Instrumentation instrumentation = new ChainedInstrumentation(
                singletonList(new TracingInstrumentation())
        );

        GraphQL graphQL = GraphQL
                .newGraphQL(buildSchema())
                .instrumentation(instrumentation)
                .build();

        runCasesQuery(graphQL);
        subscribeToCasesEvent(graphQL);
    }

    private static void subscribeToCasesEvent(GraphQL graphQL) throws InterruptedException {
        String query = "subscription {" +
                "  cases { description user { name } } }";

        DataLoaderRegistry dlr = createDataLoaderRegistry();

        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                .dataLoaderRegistry(dlr)
                .query(query)
                .build();

        ExecutionResult executionResult = graphQL.execute(executionInput);

        Publisher<ExecutionResult> stream = executionResult.getData();

        stream.subscribe(new Subscriber<ExecutionResult>() {

            @Override
            public void onSubscribe(Subscription s) {
                System.out.println("on subscribe");
                subscriptionRef.set(s);
                request(1);
            }

            @Override
            public void onNext(ExecutionResult er) {
                Object data = er.getData();
                System.out.println("on next data="+data);
//                for (var dl : dlr.getDataLoaders()) {
//                    dl.clearAll();
//                }
                request(1);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("on error");
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("on complete");
            }
        });


        System.out.println("waiting for event...");

        Thread.sleep(60000);
    }

    private static void runCasesQuery(GraphQL graphQL) {
        System.out.println("running getCases query");

        ExecutionInput executionInput2 = ExecutionInput.newExecutionInput()
                .dataLoaderRegistry(createDataLoaderRegistry())
                .query("query { getCases { description user { name } } }")
                .build();
        ExecutionResult executionResult2 = graphQL.execute(executionInput2);
        System.out.println("getCases query data: " + executionResult2.getData());
    }

    private static DataLoaderRegistry createDataLoaderRegistry() {
        BatchLoader<String, User> userBatchLoader = new BatchLoader<String, User>() {
            @Override
            public CompletionStage<List<User>> load(List<String> userIds) {
                return CompletableFuture.supplyAsync(() -> {
                    System.out.println("userBatchLoader ids: "+ String.join(", ", userIds));
                    return userIds.stream().map(id -> new User("user"+id)).toList();
                });
            }
        };

        DataLoader<String, User> userLoader = DataLoaderFactory.newDataLoader(userBatchLoader);

        DataLoaderRegistry dlr = DataLoaderRegistry.newRegistry()
                .register("user", userLoader)
                .build();
        return dlr;
    }

    private static GraphQLSchema buildSchema() {
        Reader streamReader = loadSchemaFile("schema.graphql");
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(streamReader);

        RuntimeWiring wiring = RuntimeWiring.newRuntimeWiring()
                .type(newTypeWiring("Subscription").dataFetcher("cases", casesSubscriptionFetcher()))
                .type(newTypeWiring("Query").dataFetcher("getCases", getCasesQueryFetcher()))
                .type(newTypeWiring("Case").dataFetcher("user", userFieldFetcher()))
                .build();

        return new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    }

    private static DataFetcher userFieldFetcher() {
        return environment -> {
            try {
                String userId = ((Case)environment.getSource()).getUserId();
                return environment.getDataLoader("user").load(userId);
            }
            catch (Throwable t) {
                t.printStackTrace();
                throw t;
            }
        };
    }

    private static DataFetcher casesSubscriptionFetcher() {
        return environment -> {
            Observable<List<Case>> observable = Observable.create(emitter -> {

                ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
//                executorService.scheduleAtFixedRate(newCases(emitter), 0, 3, TimeUnit.SECONDS);
                executorService.schedule(newCases(emitter), 2, TimeUnit.SECONDS);
            });

            ConnectableObservable<List<Case>> connectableObservable = observable.share().publish();
            connectableObservable.connect();

            Flowable<List<Case>> publisher = connectableObservable.toFlowable(BackpressureStrategy.BUFFER);

            return publisher;
        };
    }

    private static DataFetcher getCasesQueryFetcher() {
        return environment -> {
            return Lists.newArrayList(
                    new Case("case1", "1"),
                    new Case("case2", "2"),
                    new Case("case3", "3"),
                    new Case("case4", "4"),
                    new Case("case5", "5")
            );
        };
    }

    private static Runnable newCases(ObservableEmitter<List<Case>> emitter) {
        return () -> {
            List<Case> cases = Lists.newArrayList(
                    new Case("case1", "1"),
                    new Case("case2", "2"),
                    new Case("case3", "3"),
                    new Case("case4", "4"),
                    new Case("case5", "5")
            );
            emitter.onNext(cases);
        };
    }

    private static void request(int n) {
        Subscription subscription = subscriptionRef.get();
        if (subscription != null) {
            subscription.request(n);
        }
    }

    private static Reader loadSchemaFile(String name) {
        InputStream stream = Main.class.getClassLoader().getResourceAsStream(name);
        return new InputStreamReader(stream);
    }
}
