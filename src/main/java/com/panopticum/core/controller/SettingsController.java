package com.panopticum.core.controller;

import com.panopticum.config.HxRedirectFilter;
import com.panopticum.core.model.DbConnection;
import com.panopticum.core.service.ConnectionTestService;
import com.panopticum.core.service.DbConnectionFactory;
import com.panopticum.core.service.DbConnectionService;
import com.panopticum.prometheus.service.PrometheusService;
import com.panopticum.s3.service.S3Service;
import com.panopticum.i18n.LocaleFilter;
import com.panopticum.i18n.Messages;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.views.ModelAndView;
import io.micronaut.views.View;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Controller("/settings")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.BLOCKING)
@RequiredArgsConstructor
@Slf4j
public class SettingsController {

    private static final String HX_REQUEST = "HX-Request";

    private final DbConnectionService dbConnectionService;
    private final DbConnectionFactory dbConnectionFactory;
    private final ConnectionTestService connectionTestService;
    private final S3Service s3Service;
    private final PrometheusService prometheusService;
    @Value("${panopticum.admin-lock:false}")
    private boolean adminLock;

    @PostConstruct
    void logAdminLock() {
        log.info("Admin lock is {}", adminLock);
    }

    @Produces(MediaType.TEXT_HTML)
    @Get
    @View("settings/index")
    public Map<String, Object> index() {
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        model.put("adminLock", adminLock);

        return model;
    }

    @Produces(MediaType.TEXT_HTML)
    @Get("/edit/{id}")
    @View("settings/index")
    public Map<String, Object> edit(@PathVariable Long id) {
        assertNotLocked();
        DbConnection conn = dbConnectionService.findById(id)
                .orElseThrow(() -> new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound"));
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        model.put("adminLock", adminLock);
        model.put("editingConnection", conn);

        return model;
    }

    private void assertNotLocked() {
        if (adminLock) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "admin.lock.enabled");
        }
    }

    @Post("/add-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addPostgres(HttpRequest<?> request,
                             String name, String host, Integer port, String database, String username, String password,
                             Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("postgresql", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/pg/" + conn.getId());
    }

    @Post("/test-postgres")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testPostgres(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "postgresql", host, port, database, username, password, id);
    }

    @Post("/add-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMongo(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password,
                          Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("mongodb", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/mongo/" + conn.getId());
    }

    @Post("/test-mongo")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMongo(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "mongodb", host, port, database, username, password, id);
    }

    @Post("/add-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addRedis(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String password,
                          Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("redis", id, name, host, port, database, null, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/redis/" + conn.getId());
    }

    @Post("/test-redis")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testRedis(HttpRequest<?> request,
            String host, Integer port, String database, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "redis", host, port, database, null, password, id);
    }

    @Post("/add-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addClickhouse(HttpRequest<?> request,
                               String name, String host, Integer port, String database, String username, String password,
                               Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("clickhouse", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/clickhouse/" + conn.getId());
    }

    @Post("/test-clickhouse")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testClickhouse(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "clickhouse", host, port, database, username, password, id);
    }

    @Post("/add-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMysql(HttpRequest<?> request,
                          String name, String host, Integer port, String database, String username, String password,
                          Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("mysql", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/mysql/" + conn.getId());
    }

    @Post("/test-mysql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMysql(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "mysql", host, port, database, username, password, id);
    }

    @Post("/add-mssql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addMssql(HttpRequest<?> request,
                           String name, String host, Integer port, String database, String username, String password,
                           Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("sqlserver", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/mssql/" + conn.getId());
    }

    @Post("/test-mssql")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testMssql(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "sqlserver", host, port, database, username, password, id);
    }

    @Post("/add-oracle")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addOracle(HttpRequest<?> request,
                            String name, String host, Integer port, String database, String username, String password,
                            Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("oracle", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/oracle/" + conn.getId());
    }

    @Post("/test-oracle")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testOracle(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "oracle", host, port, database, username, password, id);
    }

    @Post("/add-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addCassandra(HttpRequest<?> request,
                              String name, String host, Integer port, String database, String username, String password,
                              Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("cassandra", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/cassandra/" + conn.getId());
    }

    @Post("/add-rabbitmq")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addRabbitmq(HttpRequest<?> request,
                              String name, String host, Integer port, String database, String username, String password,
                              Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("rabbitmq", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/rabbitmq/" + conn.getId() + "/queues");
    }

    @Post("/test-rabbitmq")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testRabbitmq(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "rabbitmq", host, port, database, username, password, id);
    }

    @Post("/add-kafka")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addKafka(HttpRequest<?> request,
                           String name, String host, Integer port, String database,
                           Optional<String> username, Optional<String> password,
                           Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("kafka", id, name, host, port, database,
                username.orElse(null), password.orElse(null));
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/kafka/" + conn.getId() + "/topics");
    }

    @Post("/test-kafka")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testKafka(HttpRequest<?> request,
            String host, Integer port, String database,
            Optional<String> username, Optional<String> password,
            Optional<Long> id) {
        return testConnectionResult(request, "kafka", host, port, database,
                username.orElse(null), password.orElse(null), id);
    }

    @Post("/add-elasticsearch")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addElasticsearch(HttpRequest<?> request,
                                   String name, String host, Integer port, String database,
                                   String username, String password,
                                   Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("elasticsearch", id, name, host, port, database, username, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/elasticsearch/" + conn.getId() + "/indices");
    }

    @Post("/add-kubernetes")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addKubernetes(HttpRequest<?> request,
                                String name, String host, Integer port, String database, String password,
                                Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdate("kubernetes", id, name, host, port, database, null, password);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/kubernetes/" + conn.getId() + "/namespaces");
    }

    @Post("/test-kubernetes")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testKubernetes(HttpRequest<?> request,
            String host, Integer port, String database, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "kubernetes", host, port, database, null, password, id);
    }

    @Post("/add-s3")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addS3(HttpRequest<?> request,
                        String name, String host, Integer port, String database,
                        String username, String password, Optional<Boolean> useHttps,
                        Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdateWithHttps("s3", id, name, host, port, database, username, password, useHttps.orElse(false));
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/s3/" + conn.getId() + "/buckets");
    }

    @Post("/test-s3")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testS3(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Boolean> useHttps, Optional<Long> id) {
        String pwd = resolvePasswordForTest(id, password);
        boolean https = useHttps.orElse(false);
        Map<String, Object> model = new HashMap<>();
        try {
            Optional<String> error = s3Service.testConnection(host, port != null ? port : 443, database, username, pwd, https);
            model.put("success", error.isEmpty());
            String messageKey = error.orElse("connectionTest.success");
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        } catch (Exception e) {
            model.put("success", false);
            String messageKey = e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed";
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        }
        return new ModelAndView<>("partials/connection-test-result", model);
    }

    @Post("/add-prometheus")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object addPrometheus(HttpRequest<?> request,
                                String name, String host, Integer port,
                                String username, String password, Optional<Boolean> useHttps,
                                Optional<Long> id) {
        assertNotLocked();
        DbConnection conn = saveOrUpdateWithHttps("prometheus", id, name, host, port, null, username, password, useHttps.orElse(false));
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());

        return responseAfterAdd(request, model, conn.getId(), "/prometheus/" + conn.getId() + "/metrics");
    }

    @Post("/test-prometheus")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testPrometheus(HttpRequest<?> request,
            String host, Integer port, String username, String password,
            Optional<Boolean> useHttps, Optional<Long> id) {
        String pwd = resolvePasswordForTest(id, password);
        boolean https = useHttps.orElse(false);
        Map<String, Object> model = new HashMap<>();
        try {
            Optional<String> error = prometheusService.testConnection(host, port != null ? port : 9090, username, pwd, https);
            model.put("success", error.isEmpty());
            String messageKey = error.orElse("connectionTest.success");
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        } catch (Exception e) {
            model.put("success", false);
            String messageKey = e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed";
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        }
        return new ModelAndView<>("partials/connection-test-result", model);
    }

    @Post("/test-elasticsearch")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testElasticsearch(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "elasticsearch", host, port, database, username, password, id);
    }

    @Post("/test-cassandra")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public ModelAndView<Map<String, Object>> testCassandra(HttpRequest<?> request,
            String host, Integer port, String database, String username, String password,
            Optional<Long> id) {
        return testConnectionResult(request, "cassandra", host, port, database, username, password, id);
    }

    @Delete("/delete-connection/{id}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Object deleteConnection(HttpRequest<?> request, @PathVariable Long id) {
        assertNotLocked();
        dbConnectionService.deleteById(id);
        Map<String, Object> model = new HashMap<>();
        model.put("connections", dbConnectionService.findAll());
        model.put("adminLock", adminLock);

        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().getFirst(HX_REQUEST).orElse(""));

        if (hxRequest) {
            return new ModelAndView<>("partials/sidebar", model);
        }

        return HttpResponse.redirect(URI.create("/settings"));
    }

    private DbConnection saveOrUpdateWithHttps(String expectedType, Optional<Long> id,
                                              String name, String host, Integer port, String database,
                                              String username, String password, boolean useHttps) {
        if (id != null && id.isPresent()) {
            DbConnection existing = dbConnectionService.findById(id.get())
                    .orElseThrow(() -> new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound"));
            if (!expectedType.equalsIgnoreCase(existing.getType())) {
                throw new HttpStatusException(HttpStatus.BAD_REQUEST, "connection.typeMismatch");
            }
            String pwd = (password != null && !password.isBlank()) ? password : existing.getPassword();
            DbConnection conn = dbConnectionFactory.build(expectedType, name, host, port, database, username, pwd);
            conn.setId(id.get());
            conn.setUseHttps(useHttps);
            return dbConnectionService.save(conn);
        }
        DbConnection conn = dbConnectionFactory.build(expectedType, name, host, port, database, username, password);
        conn.setUseHttps(useHttps);
        return dbConnectionService.save(conn);
    }

    private DbConnection saveOrUpdate(String expectedType, Optional<Long> id,
                                     String name, String host, Integer port, String database,
                                     String username, String password) {
        if (id != null && id.isPresent()) {
            DbConnection existing = dbConnectionService.findById(id.get())
                    .orElseThrow(() -> new HttpStatusException(HttpStatus.NOT_FOUND, "connection.notFound"));
            if (!expectedType.equalsIgnoreCase(existing.getType())) {
                throw new HttpStatusException(HttpStatus.BAD_REQUEST, "connection.typeMismatch");
            }
            String pwd = (password != null && !password.isBlank()) ? password : existing.getPassword();
            DbConnection conn = dbConnectionFactory.build(expectedType, name, host, port, database, username, pwd);
            conn.setId(id.get());
            conn.setUseHttps(existing.isUseHttps());
            return dbConnectionService.save(conn);
        }
        DbConnection conn = dbConnectionFactory.build(expectedType, name, host, port, database, username, password);
        return dbConnectionService.save(conn);
    }

    private Object responseAfterAdd(HttpRequest<?> request, Map<String, Object> model,
                                    Long savedId, String redirectPath) {
        model.put("adminLock", adminLock);
        boolean hxRequest = "true".equalsIgnoreCase(request.getHeaders().getFirst(HX_REQUEST).orElse(""));
        if (hxRequest) {
            if (savedId != null) {
                request.setAttribute(HxRedirectFilter.HX_REDIRECT_ATTR, redirectPath);
            }
            return new ModelAndView<>("partials/sidebar", model);
        }
        return new ModelAndView<>("settings/index", model);
    }

    private String resolvePasswordForTest(Optional<Long> id, String password) {
        if (password != null && !password.isBlank()) {
            return password;
        }
        if (id != null && id.isPresent()) {
            return dbConnectionService.findById(id.get())
                    .map(DbConnection::getPassword)
                    .orElse(password);
        }
        return password;
    }

    private ModelAndView<Map<String, Object>> testConnectionResult(HttpRequest<?> request, String type,
                                                                   String host, Integer port, String database,
                                                                   String username, String password,
                                                                   Optional<Long> id) {
        String pwd = resolvePasswordForTest(id, password);
        Map<String, Object> model = new HashMap<>();
        try {
            var error = connectionTestService.test(type, host, port, database, username, pwd, id);
            model.put("success", error.isEmpty());
            String messageKey = error.orElse("connectionTest.success");
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        } catch (Exception e) {
            model.put("success", false);
            String messageKey = e.getMessage() != null ? e.getMessage() : "error.queryExecutionFailed";
            model.put("message", messageKey);
            putDisplayText(model, request, messageKey);
        }
        return new ModelAndView<>("partials/connection-test-result", model);
    }

    private void putDisplayText(Map<String, Object> model, HttpRequest<?> request, String messageKey) {
        String locale = (String) request.getAttribute(LocaleFilter.LOCALE_ATTR).orElse("en");
        String[] parts = messageKey.split("\\|", -1);
        String key = parts[0];
        String template = Messages.forLocale(locale).getOrDefault(key, key);
        String text = parts.length > 1
                ? MessageFormat.format(template, (Object[]) Arrays.copyOfRange(parts, 1, parts.length))
                : template;
        model.put("displayText", text);
    }
}
