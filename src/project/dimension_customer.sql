CREATE Table IF NOT EXISTS `{project_id}.{dataset}.DIMCustomer` (
    CustomerID INT64 NOT NULL,
    CustomerName STRING,
    Address STRING,
    CityID INT64 NOT NULL,
    CityName STRING,
    StateID INT64 NOT NULL,
    StateName STRING,
    Age STRING,
    Gender STRING,
    Email STRING
);