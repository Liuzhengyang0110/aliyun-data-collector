package cn.kjky.collector;

import cn.kjky.collector.config.ConfigLoader;
import cn.kjky.collector.runtime.CollectorDaemon;
import cn.kjky.collector.service.CollectorEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CollectorApplicationTest {

    @TempDir
    Path tempDir;

    @Test
    void controlledConfigurationAllowsLocalPreflightCommands() throws IOException {
        Path config = controlledConfig();

        assertThat(CollectorApplication.run(new String[]{"validate-config", "--config", config.toString()})).isZero();
        assertThat(CollectorApplication.run(new String[]{"dry-run", "--config", config.toString(),
                "--start", "2026-10-13T00:00:00Z", "--end", "2026-10-13T00:10:00Z"})).isZero();
    }

    @Test
    void controlledConfigurationStillRejectsCollectionAndCloudDiagnostics() throws IOException {
        Path config = controlledConfig();

        for (String command : new String[]{"collect", "resume", "probe", "discover"}) {
            ByteArrayOutputStream stderr = new ByteArrayOutputStream();
            PrintStream previous = System.err;
            try {
                System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
                assertThat(CollectorApplication.run(new String[]{command, "--config", config.toString()}))
                        .as(command).isEqualTo(2);
            } finally {
                System.setErr(previous);
            }
            assertThat(stderr.toString(StandardCharsets.UTF_8)).contains("绕过人工开始门禁");
        }
    }

    @Test
    void manualStartDisabledKeepsOrdinaryOneShotCliBehavior() throws IOException {
        String yaml = Files.readString(Path.of("src/test/resources/config/p0-02-valid.yaml"))
                .replace("runtime:\n", "runtime:\n  requireManualStart: false\n");
        Path config = tempDir.resolve("ordinary.yaml");
        Files.writeString(config, yaml);

        assertThat(CollectorApplication.run(new String[]{"validate-config", "--config", config.toString()})).isZero();
    }

    @Test
    void controlledDaemonConstructorInitializesPauseStateBeforeStartingLoops() throws Exception {
        String dataRoot = tempDir.resolve("daemon-data").toString().replace("\\", "/");
        String yaml = controlledYaml()
                .replace("dataRoot: \"./target/config-test-data\"", "dataRoot: \"" + dataRoot + "\"")
                .replace("mtlsEnabled: true", "mtlsEnabled: false")
                .replace("https://cc-control-service.cc-system.svc.cluster.local", "http://127.0.0.1:1")
                .replace("https://cc-ingest.cc-system.svc.cluster.local", "http://127.0.0.1:1");
        Path configPath = tempDir.resolve("daemon.yaml");
        Files.writeString(configPath, yaml);

        var config = new ConfigLoader().load(configPath);
        var daemon = new CollectorDaemon(config, new CollectorEngine(config));
        Path pauseState = Path.of(config.effectiveDataRoot(), config.effectiveStateDirectory(),
                "manual-pause-state.json");
        assertThat(Files.readString(pauseState))
                .contains("\"global_paused\":true")
                .contains("\"config_version\":0");
        daemon.start();
        Thread.sleep(100);
        daemon.close();
    }

    private Path controlledConfig() throws IOException {
        String yaml = controlledYaml();
        Path config = tempDir.resolve("controlled.yaml");
        Files.writeString(config, yaml);
        return config;
    }

    private String controlledYaml() throws IOException {
        return Files.readString(Path.of("src/test/resources/config/p0-02-valid.yaml"))
                .replace("runtime:\n", "runtime:\n  requireManualStart: true\n")
                .replace("  rawPersistence:\n    enabled: true", "  rawPersistence:\n    enabled: false")
                .replace("control:\n  enabled: false", "control:\n  enabled: true");
    }
}
