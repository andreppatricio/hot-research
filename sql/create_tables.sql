/* Create tables for paper_db database */

CREATE TABLE IF NOT EXISTS papers.paper(
    api_name text,
    api_id text,
    title text UNIQUE,
    doi text,
    published_date date,
    publisher text,
    PRIMARY KEY (api_name, api_id)
);

CREATE TABLE IF NOT EXISTS papers.author(
    api_name text,
    api_id text,
    name text,
    FOREIGN KEY (api_name, api_id) REFERENCES papers.paper (api_name, api_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS  papers.journal(
    api_name text,
    api_id text,
    issn text,
    name text,
    FOREIGN KEY (api_name, api_id) REFERENCES papers.paper (api_name, api_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS papers.keyword(
    week_start_date date NOT NULL,
    n integer NOT NULL,
    word text NOT NULL,
    count integer NOT NULL,
    week_percentage real,
    PRIMARY KEY (week_start_date, word)
);