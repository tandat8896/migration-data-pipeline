{
  description = "Supabase Management Tools Environment - Stable 25.11";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pkgs.supabase-cli 
          pkgs.postgresql_15 
          pkgs.pgcli
        ];

        shellHook = ''
          echo "⚡ Supabase Tools Environment Ready! (nixos-25.11)"
          echo "------------------------------------------------"
          echo "  - pgcli \$SUPABASE_DB_URL : Kết nối vào DB"
          echo "------------------------------------------------"
        '';
      };
    };
}
