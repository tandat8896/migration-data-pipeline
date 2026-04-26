{
  description = "Kafka Infrastructure for Debezium Lab";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.openjdk11
          pkgs.apacheKafka   # Cài đặt Kafka + Zookeeper binary
          pkgs.kcat          # (Thay cho kafkacat) Để soi message CDC cực nhanh
        ];

        shellHook = ''
          # Tạo thư mục chứa data cho Kafka/Zookeeper ngay trong project
          export KAFKA_DATA="$PWD"

          echo "✓ Kafka Lab Ready"
          echo "Dùng 'zookeeper-server-start' và 'kafka-server-start' để khởi chạy"
          echo "Debezium sẽ connect tới localhost:9092"
        '';
      };
    };
}
