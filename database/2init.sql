-- Companies table
CREATE TABLE companies
(
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(255) NOT NULL UNIQUE,
    domain     VARCHAR(200),
    website    VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Job catalog table  
CREATE TABLE job_catalog
(
    id                 SERIAL PRIMARY KEY,
    djinni_id          INTEGER      NOT NULL,
    url                VARCHAR(500) NOT NULL,

    page_number        INTEGER      NOT NULL,
    position_on_page   INTEGER      NOT NULL,
    global_position    INTEGER,

    catalog_scraped_at TIMESTAMP DEFAULT NOW(),
    is_processed       BOOLEAN   DEFAULT FALSE,

    UNIQUE (djinni_id, catalog_scraped_at)
);

-- Jobs table
CREATE TABLE jobs
(
    id                    SERIAL PRIMARY KEY,
    djinni_id             INTEGER UNIQUE      NOT NULL,
    url                   VARCHAR(500) UNIQUE NOT NULL,

    catalog_id            INTEGER REFERENCES job_catalog (id),

    title                 VARCHAR(500)        NOT NULL,
    role                  VARCHAR(200),
    company_id            INTEGER REFERENCES companies (id),

    salary_min            INTEGER,
    salary_max            INTEGER,
    salary_currency       VARCHAR(10) DEFAULT 'USD',

    location              VARCHAR(300),
    remote_info           VARCHAR(300),
    countries             TEXT,

    experience_required   TEXT,
    language_requirements TEXT,

    domain                VARCHAR(200),
    product_type          VARCHAR(200),
    hiring_type           VARCHAR(200),

    description           TEXT,
    tags                  TEXT,

    all_time_views        INTEGER     DEFAULT 0,
    all_time_applications INTEGER     DEFAULT 0,
    all_time_read         INTEGER     DEFAULT 0,
    all_time_responded    INTEGER     DEFAULT 0,
    last_views            INTEGER     DEFAULT 0,
    last_applications     INTEGER     DEFAULT 0,
    last_read             INTEGER     DEFAULT 0,
    last_responded        INTEGER     DEFAULT 0,

    posted_date           DATE,
    is_active             BOOLEAN     DEFAULT TRUE,
    created_at            TIMESTAMP   DEFAULT NOW(),
    updated_at            TIMESTAMP   DEFAULT NOW()
);

-- Job skills table
CREATE TABLE job_skills
(
    id                  SERIAL PRIMARY KEY,
    job_id              INTEGER REFERENCES jobs (id) ON DELETE CASCADE,
    skill_name          VARCHAR(100) NOT NULL,
    experience_required VARCHAR(100),
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Performance indexes (только нужные)
CREATE INDEX idx_jobs_djinni_id ON jobs(djinni_id);
CREATE INDEX idx_jobs_company_id ON jobs(company_id);
CREATE INDEX idx_jobs_catalog_id ON jobs(catalog_id);
CREATE INDEX idx_jobs_active ON jobs(is_active);

CREATE INDEX idx_catalog_djinni_id ON job_catalog(djinni_id);
CREATE INDEX idx_catalog_page ON job_catalog(page_number);
CREATE INDEX idx_catalog_processed ON job_catalog(is_processed);

CREATE INDEX idx_job_skills_job_id ON job_skills(job_id);
CREATE INDEX idx_job_skills_name ON job_skills(skill_name);

-- Test data для проверки
INSERT INTO companies (name, domain) VALUES 
('Test Company', 'tech') 
ON CONFLICT (name) DO NOTHING;

INSERT INTO job_catalog (djinni_id, url, page_number, position_on_page, global_position) VALUES 
(999999, 'https://djinni.co/jobs/999999-test-job/', 1, 1, 1) 
ON CONFLICT (djinni_id, catalog_scraped_at) DO NOTHING;
