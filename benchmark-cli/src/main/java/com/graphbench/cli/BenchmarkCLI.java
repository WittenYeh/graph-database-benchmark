package com.graphbench.cli;

import com.google.gson.Gson;
import com.graphbench.benchmark.BenchmarkExecutor;
import com.graphbench.benchmark.BenchmarkResult;
import com.graphbench.compiler.CompilerFactory;
import com.graphbench.compiler.CompiledWorkload;
import com.graphbench.compiler.WorkloadCompiler;
import com.graphbench.config.*;
import com.graphbench.dataset.MatrixMarketReader;
import com.graphbench.db.DatabaseAdapter;
import com.graphbench.db.DatabaseFactory;
import com.graphbench.report.BenchmarkReport;
import com.graphbench.report.ReportWriter;
import com.graphbench.workload.Workload;
import com.graphbench.workload.WorkloadGenerator;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Unified benchmark CLI that performs generate → compile → run in a single invocation.
 */
public class BenchmarkCLI {

    public static void main(String[] args) {
        if (args.length < 12) {
            printUsage();
            System.exit(1);
        }

        try {
            // Parse arguments
            String databaseConfigPath = getArgValue(args, "--database-config");
            String databaseName = getArgValue(args, "--database-name");
            String workloadConfigPath = getArgValue(args, "--workload-config");
            String datasetConfigPath = getArgValue(args, "--dataset-config");
            String datasetNameOverride = getArgValue(args, "--dataset-name");
            String dbPath = getArgValue(args, "--db-path");
            String resultDir = getArgValue(args, "--result-dir");
            String seedStr = getArgValue(args, "--seed");
            Long seed = seedStr != null ? Long.parseLong(seedStr) : null;

            System.out.println("=== Graph Database Benchmark ===");
            System.out.println("Database: " + databaseName);
            System.out.println("Database config: " + databaseConfigPath);
            System.out.println("Workload config: " + workloadConfigPath);
            System.out.println("Dataset config: " + datasetConfigPath);
            System.out.println();

            // Load configurations
            System.out.println("[1/5] Loading configurations...");
            Gson gson = new Gson();

            DatabaseConfigLoader dbConfigLoader = new DatabaseConfigLoader(databaseConfigPath);
            if (!dbConfigLoader.hasDatabase(databaseName)) {
                System.err.println("✗ Database '" + databaseName + "' not found in configuration.");
                System.err.println("  Available databases: " + String.join(", ", dbConfigLoader.getDatabaseNames()));
                System.exit(1);
            }
            DatabaseInstanceConfig dbConfig = dbConfigLoader.getDatabaseConfig(databaseName);

            WorkloadConfig workloadConfig = gson.fromJson(new FileReader(workloadConfigPath), WorkloadConfig.class);
            DatasetConfig datasetConfig = gson.fromJson(new FileReader(datasetConfigPath), DatasetConfig.class);

            String datasetName = datasetNameOverride != null ? datasetNameOverride : workloadConfig.getDataset();
            String datasetPath = datasetConfig.resolvePath(datasetName);

            System.out.println("  Dataset: " + datasetName);
            System.out.println("  Dataset path: " + datasetPath);
            System.out.println("  ✓ Configurations loaded");
            System.out.println();

            // Parse dataset
            System.out.println("[2/5] Parsing dataset...");
            MatrixMarketReader reader = new MatrixMarketReader();
            reader.read(datasetPath);
            System.out.println("  Nodes: " + reader.getNumNodes());
            System.out.println("  Edges: " + reader.getNumEdges());
            System.out.println("  ✓ Dataset parsed");
            System.out.println();

            // Generate workloads (in-memory)
            System.out.println("[3/5] Generating workloads...");
            Long seedValue = seed != null ? seed : System.currentTimeMillis();
            WorkloadGenerator generator = new WorkloadGenerator(reader, seedValue);
            List<Workload> workloads = new ArrayList<>();
            for (var taskConfig : workloadConfig.getTasks()) {
                System.out.println("  - " + taskConfig.getName() + " (" + taskConfig.getOps() + " ops)");
                Workload workload = generator.generate(taskConfig, datasetName, datasetPath);
                workloads.add(workload);
            }
            System.out.println("  ✓ " + workloads.size() + " workloads generated");
            System.out.println();

            // Compile workloads (in-memory)
            System.out.println("[4/5] Compiling workloads to " + dbConfig.getQueryLanguage() + "...");
            WorkloadCompiler compiler = CompilerFactory.create(databaseName, dbConfig);
            List<CompiledWorkload> compiledWorkloads = new ArrayList<>();
            for (Workload workload : workloads) {
                System.out.println("  - " + workload.getTaskName());
                CompiledWorkload compiled = compiler.compile(workload);
                compiledWorkloads.add(compiled);
            }
            System.out.println("  ✓ " + compiledWorkloads.size() + " workloads compiled");
            System.out.println();

            // Execute benchmarks
            System.out.println("[5/5] Running benchmarks...");
            DatabaseAdapter db = DatabaseFactory.create(databaseName, dbConfig);
            System.out.println("  Opening embedded " + databaseName + " at: " + dbPath);
            db.open(dbPath, dbConfig);

            BenchmarkExecutor executor = new BenchmarkExecutor(db, workloadConfig.getServerConfig().getThreads());
            List<BenchmarkResult> results = executor.executeAll(compiledWorkloads);

            db.close();
            System.out.println("  ✓ All benchmarks completed");
            System.out.println();

            // Write results
            Files.createDirectories(Paths.get(resultDir));
            BenchmarkReport.Metadata metadata = new BenchmarkReport.Metadata();
            metadata.setDatabase(databaseName);
            metadata.setDataset(datasetName);
            metadata.setDatasetPath(datasetPath);
            metadata.setServerThreads(workloadConfig.getServerConfig().getThreads());
            metadata.setTimestamp(java.time.Instant.now().toString());
            metadata.setJavaVersion(System.getProperty("java.version"));
            metadata.setOsName(System.getProperty("os.name"));
            metadata.setOsVersion(System.getProperty("os.version"));

            BenchmarkReport report = new BenchmarkReport(metadata, results);
            String reportPath = ReportWriter.write(report, resultDir);
            System.out.println("=== Benchmark Complete ===");
            System.out.println("Results written to: " + reportPath);

        } catch (Exception e) {
            System.err.println("✗ Benchmark failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String getArgValue(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static void printUsage() {
        System.err.println("Usage: BenchmarkCLI [options]");
        System.err.println();
        System.err.println("Required options:");
        System.err.println("  --database-config <path>   Path to database-config.json");
        System.err.println("  --database-name <name>     Database name (must be registered in config)");
        System.err.println("  --workload-config <path>   Path to workload_config.json");
        System.err.println("  --dataset-config <path>    Path to datasets.json");
        System.err.println("  --db-path <path>           Path for database storage");
        System.err.println("  --result-dir <path>        Directory for result JSON files");
        System.err.println();
        System.err.println("Optional options:");
        System.err.println("  --dataset-name <name>      Override dataset name from workload config");
        System.err.println("  --seed <number>            Random seed for reproducibility");
    }
}
