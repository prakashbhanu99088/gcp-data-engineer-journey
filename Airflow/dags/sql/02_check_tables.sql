Select table_name
FROM information_schema.tables
WHERE table_schema='retail_dw'
ORDER BY table_name;