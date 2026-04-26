{
  description = "Kafka Infrastructure + Schema Registry for Debezium Lab";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      # Confluent Schema Registry derivation
      confluentVersion = "7.5.0";
      schemaRegistry = pkgs.stdenv.mkDerivation {
        pname = "confluent-schema-registry";
        version = confluentVersion;

        src = pkgs.fetchurl {
          url = "https://packages.confluent.io/archive/7.5/confluent-community-${confluentVersion}.tar.gz";
          sha256 = "sha256-UguDRFRERD0FrSsB4WScikPxbG89OrXZbbrVKYvl8Qg=";
        };

        buildInputs = [ pkgs.openjdk11 pkgs.makeWrapper ];

        # Không build gì cả, chỉ extract
        dontBuild = true;

        installPhase = ''
          mkdir -p $out

          # Copy everything to preserve folder structure
          cp -r bin $out/
          cp -r share $out/
          [ -d etc ] && cp -r etc $out/ || true
          [ -d lib ] && cp -r lib $out/ || true

          # Wrap executables
          for script in $out/bin/*; do
            [ -f "$script" ] && [ -x "$script" ] && wrapProgram "$script" \
              --set JAVA_HOME "${pkgs.openjdk11}" \
              --prefix PATH : "${pkgs.openjdk11}/bin"
          done
        '';

        meta = {
          description = "Confluent Schema Registry";
          homepage = "https://www.confluent.io/";
        };
      };

    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.openjdk11
          pkgs.apacheKafka   # Kafka + Zookeeper binary
          pkgs.kcat          # Kafka message inspector
          schemaRegistry     # Confluent Schema Registry
          pkgs.curl
          pkgs.jq
        ];

        shellHook = ''
          export KAFKA_DATA="$PWD"
          export SCHEMA_REGISTRY_HOME="${schemaRegistry}"
          export PATH="${schemaRegistry}/bin:$PATH"

          # Fix Schema Registry logs directory (nix store is read-only)
          export LOG_DIR="$PWD/schema_registry_confluent/logs"
          mkdir -p "$LOG_DIR"

          # Set log4j config for Schema Registry
          export SCHEMA_REGISTRY_LOG4J_OPTS="-Dlog4j.configuration=file:$PWD/schema_registry_confluent/log4j.properties"

          echo "✓ Kafka + Schema Registry Lab Ready"
          echo "Debezium sẽ connect tới localhost:9092"
          echo "Schema Registry home: $SCHEMA_REGISTRY_HOME"
          echo "Logs: $LOG_DIR"
        '';
      };
    };
}
