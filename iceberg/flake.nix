{
  description = "Iceberg Data Lakehouse - Clean & Stable Spark 3.5.1";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      spark = pkgs.stdenv.mkDerivation {
        name = "spark-3.5.1-bin";

        src = pkgs.fetchurl {
          url = "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz";
          sha256 = "sha256-XfFfgCcGfAYP5H69NRoUMaYdvsycKLjdKeLG4ZNcI+s=";
        };

        installPhase = ''
          mkdir -p $out
          cp -r ./* $out/
        '';
      };

      spark-iceberg-script = pkgs.writeShellScriptBin "spark-iceberg" ''
        set -e

        unset CLASSPATH
        unset SPARK_DIST_CLASSPATH
        unset HADOOP_HOME
        unset HADOOP_CONF_DIR

        export SPARK_HOME=${spark}

        export PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.spark:spark-avro_2.12:3.5.1,io.confluent:kafka-avro-serializer:7.5.0,za.co.absa:abris_2.12:6.4.0"

        export REPOSITORIES="https://packages.confluent.io/maven/,https://repo1.maven.org/maven2/"

        exec $SPARK_HOME/bin/spark-shell \
          --packages "$PACKAGES" \
          --repositories "$REPOSITORIES" \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
          --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.local.type=hadoop \
          --conf spark.sql.catalog.local.warehouse="$PWD/warehouse" \
          "$@"
      '';
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.jdk11
          spark-iceberg-script
        ];

        shellHook = ''
          echo "Iceberg Clean Lakehouse Ready"
          echo "Spark 3.5.1 official binary"
          echo "Iceberg 1.5.0"
          echo "Catalog: local"

          mkdir -p warehouse
        '';
      };
    };
}
