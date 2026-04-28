{
  description = "FastAPI project (uv + uvicorn + jwt)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
  };

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      pythonEnv = pkgs.python312.withPackages (ps: [
        ps.fastapi
        ps.uvicorn
        ps.pydantic
        ps.pyjwt
        ps.passlib
        ps.bcrypt
        ps.sqlalchemy
        ps.asyncpg
        ps.psycopg2

        ps.python-dotenv
        ps.faker
        ps.pymongo  # For MongoDB CDC data generation
      ]);
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.uv
          pkgs.postgresql_15 # Chỉ cài để có lệnh 'psql' đi kèm
          pkgs.tree
        ];

        shellHook = ''
          echo "🚀 FastAPI Environment Ready!"
          echo "📦 Python $(python --version) | uv $(uv --version)"
          echo "🐘 Postgres tool (psql) sẵn sàng. Bạn tự quản lý DB nhé!"
        '';
      };
    };
}
