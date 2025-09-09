-- ========================
-- HUBS
-- ========================

CREATE TABLE HUB_Customers (
    CustomerId INT PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Locations (
    LocationId SERIAL PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Employees (
    EmployeeId INT PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Invoices (
    InvoiceId INT PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_InvoiceLine (
    InvoiceLineId INT PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Tracks (
    TrackId INT PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Titles (
    TitleId SERIAL PRIMARY KEY,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

-- ========================
-- LINKS
-- ========================

CREATE TABLE LINK_CustomerLocation (
    CustomerLocationId SERIAL PRIMARY KEY,
    CustomerId INT REFERENCES HUB_Customers(CustomerId),
    LocationId INT REFERENCES HUB_Locations(LocationId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_CustomerInvoice (
    CustomerInvoiceId SERIAL PRIMARY KEY,
    CustomerId INT REFERENCES HUB_Customers(CustomerId),
    InvoiceId INT REFERENCES HUB_Invoices(InvoiceId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeCustomer (
    EmployeeCustomerId SERIAL PRIMARY KEY,
    CustomerId INT REFERENCES HUB_Customers(CustomerId),
    EmployeeId INT REFERENCES HUB_Employees(EmployeeId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeInvoice (
    EmployeeInvoiceId SERIAL PRIMARY KEY,
    EmployeeId INT REFERENCES HUB_Employees(EmployeeId),
    InvoiceId INT REFERENCES HUB_Invoices(InvoiceId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeLocation (
    EmployeeLocationId SERIAL PRIMARY KEY,
    EmployeeId INT REFERENCES HUB_Employees(EmployeeId),
    LocationId INT REFERENCES HUB_Locations(LocationId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeTitle (
    EmployeeTitleId SERIAL PRIMARY KEY,
    EmployeeId INT REFERENCES HUB_Employees(EmployeeId),
    TitleId INT REFERENCES HUB_Titles(TitleId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255),
    AppointmentDate DATE
);

CREATE TABLE LINK_InvoiceInvoiceLine (
    InvoiceInvoiceLineId SERIAL PRIMARY KEY,
    InvoiceLineId INT REFERENCES HUB_InvoiceLine(InvoiceLineId),
    InvoiceId INT REFERENCES HUB_Invoices(InvoiceId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_InvoiceLineTrack (
    InvoiceLineTrackId SERIAL PRIMARY KEY,
    InvoiceLineId INT REFERENCES HUB_InvoiceLine(InvoiceLineId),
    TrackId INT REFERENCES HUB_Tracks(TrackId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

-- ========================
-- SATELLITES
-- ========================

CREATE TABLE SAT_Customer (
    SAT_CustomerId SERIAL PRIMARY KEY,
    CustomerId INT REFERENCES HUB_Customers(CustomerId),
    CustomerFirstName VARCHAR(255),
    CustomerLastName VARCHAR(255),
    CustomerPhone VARCHAR(50),
    CustomerEmail VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Location (
    SAT_LocationId SERIAL PRIMARY KEY,
    LocationId INT REFERENCES HUB_Locations(LocationId),
    CustomerAddress VARCHAR(255),
    CustomerCity VARCHAR(255),
    CustomerState VARCHAR(255),
    CustomerCountry VARCHAR(255),
    CustomerPostalCode VARCHAR(50),
    BillingPostalCode VARCHAR(50),
    BillingAddress VARCHAR(255),
    BillingCity VARCHAR(255),
    BillingState VARCHAR(255),
    BillingCountry VARCHAR(255),
    EmployeeAddress VARCHAR(255),
    EmployeeCity VARCHAR(255),
    EmployeeState VARCHAR(255),
    EmployeeCountry VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Employee (
    SAT_EmployeeId SERIAL PRIMARY KEY,
    EmployeeId INT REFERENCES HUB_Employees(EmployeeId),
    EmployeeFirstName VARCHAR(255),
    EmployeeLastName VARCHAR(255),
    EmployeeBirthDate DATE,
    EmployeeHireDate DATE,
    EmployeePhone VARCHAR(50),
    EmployeeEmail VARCHAR(255),
    EmployeeReportsTo VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Invoice (
    SAT_InvoiceId SERIAL PRIMARY KEY,
    InvoiceId INT REFERENCES HUB_Invoices(InvoiceId),
    InvoiceDate TIMESTAMP,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_InvoiceLine (
    SAT_InvoiceLineId SERIAL PRIMARY KEY,
    InvoiceLineId INT REFERENCES HUB_InvoiceLine(InvoiceLineId),
    Quantity INT,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Track (
    SAT_TrackId SERIAL PRIMARY KEY,
    TrackId INT REFERENCES HUB_Tracks(TrackId),
    UnitPrice NUMERIC(10,2),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Title (
    SAT_TitleId SERIAL PRIMARY KEY,
    TitleId INT REFERENCES HUB_Titles(TitleId),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);
