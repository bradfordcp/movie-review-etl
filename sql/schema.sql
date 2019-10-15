CREATE DATABASE etl_project;

CREATE TABLE movies
(
    id VARCHAR NOT NULL,
    name VARCHAR,
    CONSTRAINT movies_pkey PRIMARY KEY (id)
);

CREATE TABLE reviews
(
    id VARCHAR NOT NULL,
    movie_id VARCHAR,
    headline VARCHAR,
    body VARCHAR,
    created_at DATE,
    sentiment DOUBLE PRECISION,
    stars INTEGER,
    CONSTRAINT reviews_pkey PRIMARY KEY (id),
    FOREIGN KEY (movie_id) REFERENCES movies (id)
);
