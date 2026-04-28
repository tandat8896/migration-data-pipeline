{
  description = "Odoo Data Model Research with tbls";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        # Sử dụng nativeBuildInputs cho các công cụ chạy trên host
        nativeBuildInputs = with pkgs; [
          python311
          postgresql_15
          tbls         # Kiểm tra kỹ package này
          graphviz     # Để render ảnh từ tbls
        ];

        shellHook = ''
          echo "--- Odoo Data Model Lab ---"
          export PGDATA="$PWD/.db"
          # Tự động check tbls
          if command -v tbls > /dev/null; then
            echo "✅ tbls đã sẵn sàng: $(which tbls)"
          else
            echo "❌ Không tìm thấy tbls trong PATH"
          fi
        '';
      };
    };
}
