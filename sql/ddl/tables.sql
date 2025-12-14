
CREATE TABLE IF NOT EXISTS sessions (
    session_code VARCHAR(20) PRIMARY KEY NOT NULL,
    session_name TEXT NOT NULL,
    start_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS bills (
    bill_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bill_no VARCHAR(20) NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    fk_session_code VARCHAR(20) NOT NULL,
    intro_date TIMESTAMP NOT NULL,
    act_date DATE,
    FOREIGN KEY (fk_session_code) REFERENCES sessions(session_code),
    UNIQUE(bill_no, fk_session_code)
);

CREATE TABLE IF NOT EXISTS bill_documents (
    doc_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_bill_id INT NOT NULL,
    document_type VARCHAR(40) NOT NULL,
    url TEXT NOT NULL,
    doc_date TIMESTAMP,
    FOREIGN KEY (fk_bill_id) REFERENCES bills(bill_id)
);

CREATE TABLE IF NOT EXISTS legislators (
    legislator_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(15),
    email VARCHAR(120),
    address TEXT
);

CREATE TABLE IF NOT EXISTS legislator_history (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_legislator_id INT NOT NULL,
    district VARCHAR(10),
    seniority INT,
    chamber chamber,
    url TEXT NOT NULL,
    party VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE,
    FOREIGN KEY (fk_legislator_id) REFERENCES legislators(legislator_id),
    CONSTRAINT chk_dates CHECK (end_date IS NULL OR end_date >= start_date),
    UNIQUE (fk_legislator_id, chamber, start_date)
);

CREATE TABLE IF NOT EXISTS committees (
    committee_id INT NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS committee_info (
    committee_info_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_committee_id INT,
    committee_name TEXT NOT NULL,
    url TEXT,
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (fk_committee_id) REFERENCES committees(committee_id),
    CONSTRAINT chk_dates CHECK (end_date IS NULL OR end_date >= start_date),
    UNIQUE (fk_committee_id, committee_name, url, start_date)
);


CREATE TABLE IF NOT EXISTS committee_membership (
    committee_membership_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_committee_id INT NOT NULL,
    fk_legislator_id INT NOT NULL,
    membership_start DATE NOT NULL,
    membership_end DATE,
    FOREIGN KEY (fk_committee_id) REFERENCES committees(committee_id),
    FOREIGN KEY (fk_legislator_id) REFERENCES legislators(legislator_id),
    CONSTRAINT chk_dates CHECK (membership_end IS NULL OR membership_end >= membership_start),
    UNIQUE (fk_committee_id, fk_legislator_id, membership_start)
);

CREATE TABLE IF NOT EXISTS sponsors (
    sponsor_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sponsor_type sponsor_type NOT NULL,
    fk_legislator_id INT,
    fk_committee_id INT,
    fk_bill_id INT NOT NULL,
    FOREIGN KEY (fk_legislator_id) REFERENCES legislators(legislator_id),
    FOREIGN KEY (fk_committee_id) REFERENCES committees(committee_id),
    FOREIGN KEY (fk_bill_id) REFERENCES bills(bill_id),
    CONSTRAINT chk_sponsor_exclusive CHECK (
        (fk_legislator_id IS NOT NULL AND fk_committee_id IS NULL) OR
        (fk_legislator_id IS NULL AND fk_committee_id IS NOT NULL)
    ),
    UNIQUE (fk_bill_id, fk_legislator_id, fk_committee_id)
);

CREATE TABLE IF NOT EXISTS vote_events (
    vote_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_bill_id INT NOT NULL,
    vote_timestamp TIMESTAMPTZ NOT NULL,
    chamber chamber NOT NULL,
    motion_text TEXT,
    FOREIGN KEY (fk_bill_id) REFERENCES bills(bill_id),
    UNIQUE (fk_bill_id, chamber, vote_timestamp)
);

CREATE TABLE IF NOT EXISTS legislator_votes (
    vote_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_vote_event_id INT NOT NULL,
    fk_legislator_id INT NOT NULL,
    vote_cast vote_type NOT NULL,
    FOREIGN KEY (fk_vote_event_id) REFERENCES vote_events(vote_event_id),
    FOREIGN KEY (fk_legislator_id) REFERENCES legislators(legislator_id),
    UNIQUE (fk_vote_event_id, fk_legislator_id)
);

CREATE TABLE IF NOT EXISTS bill_status_history (
    bill_status_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_bill_id INT NOT NULL,
    chamber chamber,
    status_date TIMESTAMP,
    history_action TEXT,
    fk_vote_event_id INT,
    vote_action_present BOOLEAN NOT NULL,
    FOREIGN KEY (fk_bill_id) REFERENCES legislators(legislator_id),
    FOREIGN KEY (fk_vote_event_id) REFERENCES vote_events(vote_event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_legislators_name ON legislators (first_name, last_name);
