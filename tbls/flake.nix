{
  description = "Odoo Data Model Lab with tbls - NixOS 25.11";

  inputs = {
    # Sử dụng chính xác branch nixos-25.11
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        # Dùng nativeBuildInputs để các binaries được đưa vào PATH
        nativeBuildInputs = with pkgs; [
          python311
          postgresql_15
          tbls       # Công cụ phân tích database
          graphviz   # Cần để tbls render sơ đồ ERD
        ];

        shellHook = ''
          echo "--- ❄️ NixOS 25.11 DevShell Active ---"
          export PGDATA="$PWD/.db"
          
          # Kiểm tra nhanh công cụ
          if command -v tbls > /dev/null; then
            echo "✅ [tbls] $(tbls --version) đã sẵn sàng."
          else
            echo "❌ [tbls] không tìm thấy trong PATH."
          fi
        '';
      };
    };
}
