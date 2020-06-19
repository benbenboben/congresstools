

# data comes in as json files with nesting
# will add all fields that seem most relevant after quick exploration
TABLE_CREATE_BILLS = """
CREATE TABLE IF NOT EXISTS public.bills
(
    actions json,
    amendments json,
    bill_id character varying(32) COLLATE pg_catalog."default" NOT NULL,
    bill_type character varying(32) COLLATE pg_catalog."default",
    committees json,
    congress integer NOT NULL,
    cosponsors json,
    enacted_as character varying(255) COLLATE pg_catalog."default",
    history json,
    introduced_at date,
    "number" integer,
    official_title text COLLATE pg_catalog."default",
    popular_title text COLLATE pg_catalog."default",
    related_bills json,
    short_title text COLLATE pg_catalog."default",
    sponsor json,
    status character varying(255) COLLATE pg_catalog."default",
    status_at date,
    subjects json,
    subjects_top_term character varying(255) COLLATE pg_catalog."default",
    summary text COLLATE pg_catalog."default",
    titles json,
    updated_at date,
    bill_body text COLLATE pg_catalog."default",
    CONSTRAINT bills_pkey PRIMARY KEY (congress, bill_id)
)

TABLESPACE pg_default;
"""


# very verbose as it's mostly a 'data dump' from
# many fields can (and probably will) be dropped
TABLE_CREATE_MEMBERS = """
CREATE TABLE IF NOT EXISTS public.members
(
    api_uri text COLLATE pg_catalog."default",
    at_large text COLLATE pg_catalog."default",
    chamber text COLLATE pg_catalog."default" NOT NULL,
    congress text COLLATE pg_catalog."default" NOT NULL,
    contact_form text COLLATE pg_catalog."default",
    cook_pvi text COLLATE pg_catalog."default",
    crp_id text COLLATE pg_catalog."default",
    cspan_id text COLLATE pg_catalog."default",
    date_of_birth date,
    district text COLLATE pg_catalog."default",
    dw_nominate text COLLATE pg_catalog."default",
    facebook_account text COLLATE pg_catalog."default",
    fax text COLLATE pg_catalog."default",
    fec_candidate_id text COLLATE pg_catalog."default",
    first_name text COLLATE pg_catalog."default" NOT NULL,
    gender text COLLATE pg_catalog."default",
    geoid text COLLATE pg_catalog."default",
    google_entity_id text COLLATE pg_catalog."default",
    govtrack_id text COLLATE pg_catalog."default",
    icpsr_id text COLLATE pg_catalog."default",
    id text COLLATE pg_catalog."default",
    ideal_point text COLLATE pg_catalog."default",
    in_office boolean,
    last_name text COLLATE pg_catalog."default" NOT NULL,
    last_updated text COLLATE pg_catalog."default",
    leadership_role text COLLATE pg_catalog."default",
    lis_id text COLLATE pg_catalog."default",
    middle_name text COLLATE pg_catalog."default",
    missed_votes text COLLATE pg_catalog."default",
    missed_votes_pct numeric,
    next_election text COLLATE pg_catalog."default",
    ocd_id text COLLATE pg_catalog."default",
    office text COLLATE pg_catalog."default",
    party text COLLATE pg_catalog."default",
    phone text COLLATE pg_catalog."default",
    rss_url text COLLATE pg_catalog."default",
    senate_class text COLLATE pg_catalog."default",
    seniority text COLLATE pg_catalog."default",
    short_title text COLLATE pg_catalog."default",
    state text COLLATE pg_catalog."default",
    state_rank text COLLATE pg_catalog."default",
    suffix text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    total_present text COLLATE pg_catalog."default",
    total_votes text COLLATE pg_catalog."default",
    twitter_account text COLLATE pg_catalog."default",
    url text COLLATE pg_catalog."default",
    votes_against_party_pct numeric,
    votes_with_party_pct numeric,
    votesmart_id text COLLATE pg_catalog."default",
    youtube_account text COLLATE pg_catalog."default",
    CONSTRAINT members_pkey PRIMARY KEY (last_name, first_name, congress, chamber)
)

TABLESPACE pg_default;
"""

TABLE_CREATE_HOUSE_VOTES = """
CREATE TABLE IF NOT EXISTS public.house_votes
(
    rollcall integer NOT NULL,
    date date,
    bill character varying(32) COLLATE pg_catalog."default" NOT NULL,
    session integer,
    congress integer NOT NULL,
    type text COLLATE pg_catalog."default",
    result character varying(32) COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    CONSTRAINT house_votes_pkey PRIMARY KEY (congress, bill, rollcall)
)

TABLESPACE pg_default;
"""

TABLE_CREATE_HOUSE_VOTES_INDIVIDUAL = """
CREATE TABLE IF NOT EXISTS public.house_votes_individual
(
    name_id character varying(32) COLLATE pg_catalog."default",
    sort_field text COLLATE pg_catalog."default",
    unaccented_name text COLLATE pg_catalog."default",
    party character varying(32) COLLATE pg_catalog."default",
    state character varying(32) COLLATE pg_catalog."default",
    vote character varying(32) COLLATE pg_catalog."default",
    congress integer NOT NULL,
    session integer,
    rollcall integer NOT NULL,
    vote_question text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    CONSTRAINT house_votes_individual_pkey PRIMARY KEY (congress, roll_call)
)

TABLESPACE pg_default;
"""

TABLE_CREATE_SENATE_VOTES = """
CREATE TABLE IF NOT EXISTS public.house_votes_individual
(
    name_id character varying(32) COLLATE pg_catalog."default",
    sort_field text COLLATE pg_catalog."default",
    unaccented_name text COLLATE pg_catalog."default",
    party character varying(32) COLLATE pg_catalog."default",
    state character varying(32) COLLATE pg_catalog."default",
    vote character varying(32) COLLATE pg_catalog."default",
    congress integer NOT NULL,
    session integer,
    rollcall integer NOT NULL,
    vote_question text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    CONSTRAINT house_votes_individual_pkey PRIMARY KEY (congress, rollcall)
)

TABLESPACE pg_default;
"""

TABLE_CREATE_SENATE_VOTES_INDIVIDUAL = """
CREATE TABLE IF NOT EXISTS public.senate_votes_individual
(
    firstname text COLLATE pg_catalog."default",
    surname text COLLATE pg_catalog."default",
    state character varying(32) COLLATE pg_catalog."default",
    party character varying(32) COLLATE pg_catalog."default",
    vote character varying(32) COLLATE pg_catalog."default",
    congress integer,
    session integer,
    question text COLLATE pg_catalog."default",
    required_majority numeric,
    rollcall integer
)

TABLESPACE pg_default;
"""