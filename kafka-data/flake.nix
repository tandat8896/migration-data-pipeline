{
  description = "Kafka Infrastructure + Schema Registry for Debezium Lab";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      # Apicurio Registry derivation
      apicurioVersion = "2.6.11.Final";
      apicurioRegistry = pkgs.stdenv.mkDerivation {
        pname = "apicurio-registry";
        version = apicurioVersion;

        src = pkgs.fetchurl {
          url = "https://github.com/Apicurio/apicurio-registry/releases/download/${apicurioVersion}/apicurio-registry-app-${apicurioVersion}-all.tar.gz";
          sha256 = "sha256-i5Huqze8HRSoforiNarNjKBkMCs6xAJ2Hls0Z+jrzxI=";
        };

        buildInputs = [ pkgs.jdk17 pkgs.makeWrapper ];

        sourceRoot = ".";
        dontBuild = true;

        installPhase = ''
          mkdir -p $out/bin $out/lib

          # Find and install JAR
          JAR_FILE=$(find . -name "*-runner.jar" -type f | head -1)
          install -m 644 "$JAR_FILE" $out/lib/apicurio-registry-runner.jar

          # Create start script
          cat > $out/bin/apicurio-registry-start << 'EOF'
#!/usr/bin/env bash
set -e
CONFIG_FILE="''${1:-$PWD/apicurio-registry/application.properties}"
echo "Starting Apicurio Registry..."
echo "Config: $CONFIG_FILE"
exec java -Dquarkus.http.port=8082 -Dquarkus.config.locations="$CONFIG_FILE" -jar APICURIO_JAR
EOF

          sed -i "s|APICURIO_JAR|$out/lib/apicurio-registry-runner.jar|g" $out/bin/apicurio-registry-start
          chmod +x $out/bin/apicurio-registry-start

          wrapProgram $out/bin/apicurio-registry-start \
            --set JAVA_HOME "${pkgs.jdk17}" \
            --prefix PATH : "${pkgs.jdk17}/bin"
        '';

        meta = {
          description = "Apicurio Registry";
          homepage = "https://www.apicur.io/registry/";
        };
      };

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
          pkgs.jdk17
          pkgs.apacheKafka   # Kafka + Zookeeper
          pkgs.kcat          # Kafka inspector
          schemaRegistry     # Confluent Schema Registry
          apicurioRegistry   # Apicurio Registry
          pkgs.curl
          pkgs.jq
        ];

        shellHook = ''
          export KAFKA_DATA="$PWD"
          export SCHEMA_REGISTRY_HOME="${schemaRegistry}"
          export APICURIO_REGISTRY_HOME="${apicurioRegistry}"
          export PATH="${schemaRegistry}/bin:${apicurioRegistry}/bin:$PATH"

          # Data directories
          mkdir -p "$PWD/apicurio-registry"
          mkdir -p "$PWD/schema_registry_confluent/logs"
          export LOG_DIR="$PWD/schema_registry_confluent/logs"
          export SCHEMA_REGISTRY_LOG4J_OPTS="-Dlog4j.configuration=file:$PWD/schema_registry_confluent/log4j.properties"

          echo "✓ Kafka + Apicurio Registry Lab"
          echo "Kafka: localhost:9092, localhost:9094"
          echo "Apicurio: apicurio-registry-start &"
          echo "Config: apicurio-registry/application.properties"
        '';
      };
    };
}
