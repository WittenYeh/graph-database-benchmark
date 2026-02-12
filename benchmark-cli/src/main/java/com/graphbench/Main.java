package com.graphbench;

import com.graphbench.cli.BenchmarkCLI;

/**
 * Main entry point for the graph database benchmark CLI.
 */
public class Main {
    public static void main(String[] args) {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("-h")) {
            printHelp();
            System.exit(0);
        }

        String command = args[0];
        String[] commandArgs = new String[args.length - 1];
        System.arraycopy(args, 1, commandArgs, 0, args.length - 1);

        switch (command) {
            case "benchmark":
                BenchmarkCLI.main(commandArgs);
                break;
            default:
                System.err.println("Unknown command: " + command);
                System.err.println("Run with --help for usage information");
                System.exit(1);
        }
    }

    private static void printHelp() {
        System.out.println("Graph Database Benchmark - Unified CLI");
        System.out.println();
        System.out.println("Usage: java -jar benchmark-cli.jar <command> [options]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  benchmark    Run a complete benchmark (generate + compile + execute)");
        System.out.println();
        System.out.println("Benchmark options:");
        System.out.println("  --database-config <path>   Path to database-config.json");
        System.out.println("  --database-name <name>     Database name (neo4j, janusgraph, etc.)");
        System.out.println("  --workload-config <path>   Path to workload_config.json");
        System.out.println("  --dataset-config <path>    Path to datasets.json");
        System.out.println("  --db-path <path>           Path for database storage");
        System.out.println("  --result-dir <path>        Directory for result JSON files");
        System.out.println("  --dataset-name <name>      (Optional) Override dataset name");
        System.out.println("  --seed <number>            (Optional) Random seed");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -jar benchmark-cli.jar benchmark \\");
        System.out.println("    --database-config database-config.json \\");
        System.out.println("    --database-name neo4j \\");
        System.out.println("    --workload-config workloads/workload_config.json \\");
        System.out.println("    --dataset-config datasets.json \\");
        System.out.println("    --db-path /tmp/neo4j-bench \\");
        System.out.println("    --result-dir reports");
    }
}
