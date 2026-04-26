{
  description = "Debezium Server Standalone - Production Grade Environment";

  # Kế thừa hạ tầng cache để tải package nhanh hơn
  nixConfig = {
    extra-substituters = "https://tandat-etl.cachix.org";
    extra-trusted-public-keys = "tandat-etl.cachix.org-1:ozcJtr36PUoskic/WYGMnYIxC/jLxr+HCBrdgyb03io=";
  };

  inputs = {
    # Dùng đúng commit hash "Production" của bạn để đồng bộ version thư viện
    nixpkgs.url = "github:NixOS/nixpkgs/107cba9eb4a8d8c9f8e9e61266d78d340867913a";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };

      # Định nghĩa Debezium Server bản thô (Raw) để học hệ thống
      debezium-raw = pkgs.stdenv.mkDerivation rec {
        pname = "debezium-server-raw";
        version = "2.5.4.Final";

        src = pkgs.fetchurl {
          url = "https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/${version}/debezium-server-dist-${version}.tar.gz";
          # Nhớ thay bằng hash thật Nix báo lỗi (got: sha256-...)
          sha256 = "sha256-iJsBnd6e7NtXyyBw4EBg3raLV0+dGmaHjXKJ5kiMD+8=";
        };

        # Tự unpack để kiểm soát
        unpackPhase = ''
          mkdir source
          tar -xzf $src -C source --strip-components=1
          cd source
        '';

        installPhase = ''
          mkdir -p $out/share/debezium
          # Cài đặt 644 (read-only) đúng tinh thần học hỏi hệ thống sâu
          find . -type f -exec install -D -m 644 {} $out/share/debezium/{} \;
        '';
      };

    in {
      devShells.${system}.default = pkgs.mkShell {
        # Chỉ cài những thứ cần thiết nhất để chạy Debezium
        packages = [
          pkgs.openjdk11  # Java Runtime
          pkgs.jq         # Tool hỗ trợ soi JSON từ CDC
          pkgs.just       # Để bạn viết các command chạy nhanh (nếu thích)
          debezium-raw
          # tree
          pkgs.tree
        ];

        shellHook = ''
          export JAVA_HOME="${pkgs.openjdk11.home}"
          export DEBEZIUM_HOME="${debezium-raw}/share/debezium"
          
          echo "✓ Debezium Production-Grade Lab Ready"
          echo "Debezium path: $DEBEZIUM_HOME"
          echo "Mode: Standard 644 (Run with 'java -jar')"
        '';
      };
    };
}
