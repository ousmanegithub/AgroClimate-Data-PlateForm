DROP TABLE IF EXISTS subventions_agricoles CASCADE;
DROP TABLE IF EXISTS infrastructures CASCADE;
DROP TABLE IF EXISTS regions CASCADE;

CREATE TABLE regions (
    id SERIAL PRIMARY KEY,
    nom_region VARCHAR(100) UNIQUE NOT NULL,
    code_region VARCHAR(10) UNIQUE NOT NULL,
    zone_agro_ecologique VARCHAR(100),
    pluviometrie_moyenne FLOAT,
    population_rurale INT
);

CREATE TABLE infrastructures (
    id SERIAL PRIMARY KEY,
    region_id INT REFERENCES regions(id),
    annee INT CHECK (annee BETWEEN 2003 AND 2012),
    nb_forages INT,
    nb_centres_stockage INT,
    km_routes_rurales FLOAT
);

CREATE TABLE subventions_agricoles (
    id SERIAL PRIMARY KEY,
    region_id INT REFERENCES regions(id),
    annee INT CHECK (annee BETWEEN 2003 AND 2012),
    montant_total_fcfa BIGINT,
    nb_beneficiaires INT,
    type_subvention VARCHAR(100)
);



INSERT INTO regions (nom_region, code_region, zone_agro_ecologique, pluviometrie_moyenne, population_rurale)
VALUES
('Dakar', 'DK', 'Zone côtière', 450, 1000000),
('Diourbel', 'DB', 'Bassin arachidier', 580, 600000),
('Fatick', 'FK', 'Zone delta saline', 650, 500000),
('Kaffrine', 'KF', 'Zone arachidière sèche', 550, 400000),
('Kaolack', 'KL', 'Bassin arachidier central', 700, 800000),
('Kédougou', 'KG', 'Zone sud-est montagneuse', 1200, 150000),
('Kolda', 'KD', 'Zone forestière sud', 1100, 500000),
('Louga', 'LG', 'Zone sylvo-pastorale', 400, 450000),
('Matam', 'MT', 'Vallée semi-aride', 300, 350000),
('Saint-Louis', 'SL', 'Vallée du fleuve', 350, 600000),
('Sédhiou', 'SD', 'Zone forestière humide', 1300, 300000),
('Tambacounda', 'TB', 'Zone soudanienne', 900, 450000),
('Thiès', 'TH', 'Zone arachidière ouest', 600, 900000),
('Ziguinchor', 'ZG', 'Zone sud tropicale', 1400, 500000);



INSERT INTO infrastructures
(region_id, annee, nb_forages, nb_centres_stockage, km_routes_rurales)
SELECT 
    r.id,
    y,
    20 + (r.id * 4) + (y - 2003) * 2,
    5 + (r.id * 1) + (y - 2003),
    100 + (r.id * 10) + (y - 2003) * 8
FROM regions r,
GENERATE_SERIES(2003, 2012) AS y;



INSERT INTO subventions_agricoles
(region_id, annee, montant_total_fcfa, nb_beneficiaires, type_subvention)
SELECT 
    r.id,
    y,
    200000000 + (r.population_rurale * 200) + (y - 2003) * 30000000,
    1000 + (r.id * 120) + (y - 2003) * 80,
    CASE 
        WHEN r.id % 3 = 0 THEN 'Engrais'
        WHEN r.id % 3 = 1 THEN 'Semences améliorées'
        ELSE 'Matériel agricole'
    END
FROM regions r,
GENERATE_SERIES(2003, 2012) AS y;