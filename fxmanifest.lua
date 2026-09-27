-- core_db — the Postgres runtime of the core framework (DESIGN §56 in core/DESIGN.md).
-- Server only. `core` declares `dependency 'core_db'`, so `ensure core` starts this first; restart `core`,
-- NEVER `core_db`, on a live server (its queue's last flush would die with its Node environment).
-- The bundle is built from src/ by build/ (npm run build -w core_db-build) and committed. No package.json and
-- no node_modules at this root: FXServer's yarn builder would run, and its Node sandbox cannot read them.

fx_version 'cerulean'
game 'gta5'

author 'Liam Robinson'
description 'core_db: Postgres pool, write-behind queue, migrations, table helpers and transactions for core and its plugins'
version '1.0.0'

node_version '22'

server_script 'dist/core_db.js'
