-- 
-- Copyright 2025 Stephen Tetley
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 


CREATE SCHEMA IF NOT EXISTS file_download;
CREATE SCHEMA IF NOT EXISTS file_download_landing;

CREATE TABLE file_download_landing.landing_files(
    qualified_table_name VARCHAR NOT NULL,
    file_name VARCHAR,
    file_path VARCHAR,
);

CREATE OR REPLACE VIEW  file_download_landing.vw_table_information AS (
WITH cte1 AS (
    -- Find `funcloc`
    SELECT DISTINCT ON (t.table_name)
        'funcloc' AS entity_type,
        t.table_name AS landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'FUNCLOC'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'TXTMI'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'FLOC_REF'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'TPLMA'
), cte2 AS (
    -- Find `classfloc`
    SELECT DISTINCT ON (t.table_name)
        'classfloc' AS entity_type,
        t.table_name as landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'FUNCLOC'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'CLASS'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'CLASSTYPE'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'LKENZ_CLA'
), cte3 AS (
    -- Find `valuafloc`
    SELECT DISTINCT ON (t.table_name)
        'valuafloc' AS entity_type,
        t.table_name as landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'FUNCLOC'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'CHARID'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'ATCOD'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'VALCNT'
), cte4 AS (
    -- Find `equi`
    SELECT DISTINCT ON (t.table_name)
        'equi' AS entity_type,
        t.table_name as landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'EQUI'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'TPLN_EILO'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'SERGE'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'HERST'
), cte5 AS (
    -- Find `classequi`
    SELECT DISTINCT ON (t.table_name)
        'classequi' AS entity_type,
        t.table_name as landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'EQUI'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'CLASS'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'CLASSTYPE'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'LKENZ_CLA'
), cte6 AS (
    -- Find `valuaequi`
    SELECT DISTINCT ON (t.table_name)
        'valuaequi' AS entity_type,
        t.table_name as landing_table,
        t.table_schema || '.' || t.table_name AS qualified_landing_table,
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'EQUI'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'CHARID'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'ATCOD'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'VALCNT'
)
SELECT * FROM cte1 
UNION
SELECT * FROM cte2
UNION
SELECT * FROM cte3
UNION
SELECT * FROM cte4
UNION
SELECT * FROM cte5
UNION
SELECT * FROM cte6
);

CREATE OR REPLACE TABLE file_download.funcloc (
    funcloc VARCHAR NOT NULL,
    abckzfloc VARCHAR,
    abckzi VARCHAR,
    answt DECIMAL(18, 3),
    ansdt DATE,
    deact VARCHAR,
    anln1_fl VARCHAR,
    anlnri VARCHAR,
    einzli VARCHAR,
    begrui VARCHAR,
    begru VARCHAR,
    cgwldt_fl DATE,
    vgwldt_fl DATE,
    gsbe_floc VARCHAR,
    gsberi VARCHAR,
    rbnr_floc VARCHAR,
    bukrsi VARCHAR,
    bukrsfloc INTEGER,
    submti VARCHAR,
    baumm VARCHAR,
    submtiflo VARCHAR,
    baujj VARCHAR,
    kokrsi VARCHAR,
    kokr_floc INTEGER,
    kost_floc INTEGER,
    kostli VARCHAR,
    herld VARCHAR,
    waers VARCHAR,
    lvorm VARCHAR,
    txtmi VARCHAR,
    usta_floc VARCHAR,
    vtweg VARCHAR,
    spart VARCHAR,
    iequii VARCHAR,
    equi_floc VARCHAR,
    fltyp INTEGER,
    brgew DECIMAL(18,3),
    cgaerb_fl VARCHAR,
    vgaerb_fl VARCHAR,
    iequi BOOLEAN,
    invnr VARCHAR,
    liznr VARCHAR,
    stor_floc VARCHAR,
    storti VARCHAR,
    gewrkfloc VARCHAR,
    ingr_floc VARCHAR,
    rbnr_i VARCHAR,
    ingrpi VARCHAR,
    swerki VARCHAR,
    swerk_fl INTEGER,
    serge VARCHAR,
    mapar VARCHAR,
    herst VARCHAR,
    floc_ref VARCHAR,
    cmganr_fl VARCHAR,
    vmganr_fl VARCHAR,
    typbz VARCHAR,
    objidfloc VARCHAR,
    objtyfloc VARCHAR,
    eqart VARCHAR,
    jobjn_fl VARCHAR,
    ppsidi VARCHAR,
    plnt_floc INTEGER,
    beber_fl VARCHAR,
    beberi VARCHAR,
    wergwfloc INTEGER,
    iwerki VARCHAR,
    posnr VARCHAR,
    trpnr1 VARCHAR,
    trpnr VARCHAR,
    msgrp VARCHAR,
    msgrpi VARCHAR,
    vkorg VARCHAR,
    vkgrp VARCHAR,
    vkbur VARCHAR,
    vkorgi VARCHAR,
    aufn_floc VARCHAR,
    aufnri VARCHAR,
    iflot_srt VARCHAR,
    einzl VARCHAR,
    groes VARCHAR,
    eqfnr VARCHAR,
    dauf_floc VARCHAR,
    daufni VARCHAR,
    inbdt DATE,
    stattext VARCHAR,
    stsm_floc VARCHAR,
    ustw_floc VARCHAR,
    uswo_floc VARCHAR,
    tplkz_flc VARCHAR,
    anla_fl VARCHAR,
    tplma1 VARCHAR,
    tplma VARCHAR,
    gewei VARCHAR,
    sttxu VARCHAR,
    datbi_flo DATE,
    proi_floc VARCHAR,
    proidi VARCHAR,
    cgwlen_fl DATE,
    vgwlen_fl DATE,
    cwaget_fl VARCHAR,
    vwaget_fl VARCHAR,
    arbplfloc VARCHAR,
    lgwidi VARCHAR,
    modeldesc VARCHAR,
    modelname VARCHAR,
    modelref VARCHAR,
    modelrver VARCHAR,
    modelver VARCHAR,
    adrnr VARCHAR,
    adrnri VARCHAR,
    geo_exist VARCHAR,
    alkey INTEGER,
    modelext VARCHAR   
);

CREATE OR REPLACE TABLE file_download.classfloc (
    funcloc VARCHAR,
    classname VARCHAR,
    classtype VARCHAR,
    lkenz_cla VARCHAR,
    clint VARCHAR,
    clstatus1 VARCHAR
);

CREATE OR REPLACE TABLE file_download.valuafloc (
    funcloc VARCHAR NOT NULL,
    ataw1 VARCHAR,
    atawe VARCHAR,
    ataut VARCHAR,
    charid VARCHAR,
    atnam VARCHAR,
    atwrt VARCHAR,
    classtype VARCHAR,
    atcod INTEGER,
    atvglart VARCHAR,
    textbez VARCHAR,
    atzis VARCHAR,
    valcnt VARCHAR,
    atimb VARCHAR,
    atsrt VARCHAR,
    atflv DECIMAL(18, 3),
    atflb DECIMAL(18, 3),
);


CREATE OR REPLACE TABLE file_download.equi (
    equi VARCHAR NOT NULL,
    abck_eilo VARCHAR,
    abckzi VARCHAR,
    answt DECIMAL(18, 3),
    ansdt DATE,
    deact VARCHAR,
    anl1_eilo VARCHAR,
    begrui VARCHAR,
    begru VARCHAR,
    char2equi VARCHAR,
    cgwldt_eq DATE,
    vgwldt_eq DATE,
    gsbe_eilo VARCHAR,
    gsberi VARCHAR,
    rbnr_eeqz VARCHAR,
    zzclass VARCHAR,
    bukrsi VARCHAR,
    bukr_eilo INTEGER,
    kmatn VARCHAR,
    baumm_eqi VARCHAR,
    subm_eeqz VARCHAR,
    baujj INTEGER,
    kokrsi VARCHAR,
    kokr_eilo INTEGER,
    kost_eilo INTEGER,
    kostli VARCHAR,
    herld VARCHAR,
    waers VARCHAR,
    kunde_eq VARCHAR,
    kunde VARCHAR,
    kun1_eeqz VARCHAR,
    gewrki VARCHAR,
    lvorm_eqi VARCHAR,
    auldt_eqi DATE,
    txtmi VARCHAR,
    usta_equi VARCHAR,
    vtweg VARCHAR,
    spart VARCHAR,
    kun2_eeqz VARCHAR,
    eqtyp VARCHAR,
    tplnr_i VARCHAR,
    tpln_eilo VARCHAR,
    brgew DECIMAL(18,3),
    cgaerb_eq VARCHAR,
    vgaerb_eq VARCHAR,
    invnr VARCHAR,
    eqasp VARCHAR,
    lsernr VARCHAR,
    liznr VARCHAR,
    stor_eilo VARCHAR,
    storti VARCHAR,
    eq_ltext VARCHAR,
    arbp_eeqz VARCHAR,
    ingr_eeqz VARCHAR,
    rbnr_i VARCHAR,
    ingrpi VARCHAR,
    swerki VARCHAR,
    swer_eilo INTEGER,
    serge VARCHAR,
    mapa_eeqz VARCHAR,
    herst VARCHAR,
    cmganr_eq VARCHAR,
    vmganr_eq VARCHAR,
    mat2equi VARCHAR,
    mat2equic VARCHAR,
    mat_equ VARCHAR,
    sernr VARCHAR,
    typbz VARCHAR,
    obji_eilo VARCHAR,
    objt_equi VARCHAR,
    eqart_equ VARCHAR,
    kun3_eeqz VARCHAR,
    ppsidi VARCHAR,
    ppla_eeqz INTEGER,
    werk_equi VARCHAR,
    bebe_eilo VARCHAR,
    beberi VARCHAR,
    wergw_eqi INTEGER,
    iwerki VARCHAR,
    heqn_eeqz VARCHAR,
    krfkz VARCHAR,
    msgr_eilo VARCHAR,
    msgrpi VARCHAR,
    vkorg VARCHAR,
    vkgrp VARCHAR,
    vkbur VARCHAR,
    vkorgi VARCHAR,
    gernr VARCHAR,
    aufn_eilo VARCHAR,
    aufnri VARCHAR,
    groes_equ VARCHAR,
    eqfn_eilo VARCHAR,
    eqfnri VARCHAR,
    dauf_eilo VARCHAR,
    daufni VARCHAR,
    inbdt DATE,
    stattext VARCHAR,
    stsm_equi VARCHAR,
    ustw_equi VARCHAR,
    uswo_equi VARCHAR,
    lager_eqi VARCHAR,
    anl2_eilo VARCHAR,
    hequ_eeqz VARCHAR,
    tidn_eeqz VARCHAR,
    gewei VARCHAR,
    data_eeqz DATE,
    datb_eeqz DATE,
    datbi_eil DATE,
    elief_eqi VARCHAR,
    proi_eilo VARCHAR,
    proidi VARCHAR,
    cgwlen_eq DATE,
    vgwlen_eq DATE,
    cwaget_eq VARCHAR,
    vwaget_eq VARCHAR,
    arbp_eilo VARCHAR,
    aineq_ind VARCHAR,
    modelid VARCHAR,
    ain_equnr VARCHAR,
    adrnr VARCHAR,
    adrnri VARCHAR,
    copy_from VARCHAR,
    -- keep `instime` as a string
    instime VARCHAR,        
    insdate DATE,
    frcrmv VARCHAR,
    frcfit VARCHAR,
    funcid VARCHAR,
    geo_exist VARCHAR,
    iuid_type VARCHAR,
    uii_agen VARCHAR,
    is_model VARCHAR,
    ppeguid VARCHAR,
    uii_plant VARCHAR,
    uii VARCHAR
);

CREATE OR REPLACE TABLE file_download.classequi (
    equi VARCHAR NOT NULL,
    classname VARCHAR,
    classtype VARCHAR,
    lkenz_cla VARCHAR,
    clint VARCHAR,
    zzstdclas BOOLEAN,
    clstatus1 VARCHAR,
);

CREATE OR REPLACE TABLE file_download.valuaequi (
    equi VARCHAR NOT NULL,
    ataw1 VARCHAR,
    atawe VARCHAR,
    ataut VARCHAR,
    charid VARCHAR,
    atnam VARCHAR,
    atwrt VARCHAR,
    classtype VARCHAR,
    atcod INTEGER,
    atvglart VARCHAR,
    textbez VARCHAR,
    atzis VARCHAR,
    valcnt VARCHAR,
    atimb VARCHAR,
    atsrt VARCHAR,
    atflv DECIMAL(18, 3),
    atflb DECIMAL(18, 3),
);
