-- ========================
-- HUBS
-- ========================

CREATE TABLE HUB_Customers (
    CustomerID SERIAL PRIMARY KEY,
    CustomerBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Company (
    CompanyID SERIAL PRIMARY KEY,
    CompanyBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Locations (
    LocationID SERIAL PRIMARY KEY,
    LocationBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Employees (
    EmployeeID SERIAL PRIMARY KEY,
    EmployeeBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Invoices (
    InvoiceID SERIAL PRIMARY KEY,
    InvoiceBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_InvoiceLine (
    InvoiceLineID SERIAL PRIMARY KEY,
    InvoiceLineBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Tracks (
    TrackID SERIAL PRIMARY KEY,
    TrackBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE HUB_Titles (
    TitleID SERIAL PRIMARY KEY,
    TitleBK VARCHAR(255) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

-- ========================
-- LINKS
-- ========================

CREATE TABLE LINK_CustomerCompany (
    CustomerCompanyID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES HUB_Customers(CustomerID),
    CompanyID INT REFERENCES HUB_Company(CompanyID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_CustomerLocation (
    CustomerLocationID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES HUB_Customers(CustomerID),
    LocationID INT REFERENCES HUB_Locations(LocationID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_CustomerInvoice (
    CustomerInvoiceID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES HUB_Customers(CustomerID),
    InvoiceID INT REFERENCES HUB_Invoices(InvoiceID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeCustomer (
    EmployeeCustomerID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES HUB_Customers(CustomerID),
    EmployeeID INT REFERENCES HUB_Employees(EmployeeID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeInvoice (
    EmployeeInvoiceID SERIAL PRIMARY KEY,
    EmployeeID INT REFERENCES HUB_Employees(EmployeeID),
    InvoiceID INT REFERENCES HUB_Invoices(InvoiceID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeLocation (
    EmployeeLocationID SERIAL PRIMARY KEY,
    EmployeeID INT REFERENCES HUB_Employees(EmployeeID),
    LocationID INT REFERENCES HUB_Locations(LocationID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_EmployeeTitle (
    EmployeeTitleID SERIAL PRIMARY KEY,
    EmployeeID INT REFERENCES HUB_Employees(EmployeeID),
    TitleID INT REFERENCES HUB_Titles(TitleID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255),
    AppointmentDate DATE
);

CREATE TABLE LINK_InvoiceInvoiceLine (
    InvoiceInvoiceLineID SERIAL PRIMARY KEY,
    InvoiceLineID INT REFERENCES HUB_InvoiceLine(InvoiceLineID),
    InvoiceID INT REFERENCES HUB_Invoices(InvoiceID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE LINK_InvoiceLineTrack (
    InvoiceLineTrackID SERIAL PRIMARY KEY,
    InvoiceLineID INT REFERENCES HUB_InvoiceLine(InvoiceLineID),
    TrackID INT REFERENCES HUB_Tracks(TrackID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

-- ========================
-- SATELLITES
-- ========================

CREATE TABLE SAT_Customer (
    SAT_CustomerID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES HUB_Customers(CustomerID),
    CustomerFirstName VARCHAR(255),
    CustomerLastName VARCHAR(255),
    CustomerPhone VARCHAR(50),
    CustomerEmail VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Company (
    SAT_CompanyID SERIAL PRIMARY KEY,
    CompanyID INT REFERENCES HUB_Company(CompanyID),
    CompanyName VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Location (
    SAT_LocationID SERIAL PRIMARY KEY,
    LocationID INT REFERENCES HUB_Locations(LocationID),
    City VARCHAR(255),
    Country VARCHAR(255),
    State VARCHAR(255),
    Address VARCHAR(255),
    PostalCode VARCHAR(50),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Employee (
    SAT_EmployeeID SERIAL PRIMARY KEY,
    EmployeeID INT REFERENCES HUB_Employees(EmployeeID),
    EmployeeFirstName VARCHAR(255),
    EmployeeLastName VARCHAR(255),
    EmployeeBirthDate DATE,
    EmployeeHireDate DATE,
    EmployeePhone VARCHAR(50),
    EmployeeEmail VARCHAR(255),
    EmployeeReport VARCHAR(255),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Invoice (
    SAT_InvoiceID SERIAL PRIMARY KEY,
    InvoiceID INT REFERENCES HUB_Invoices(InvoiceID),
    InvoiceDate DATE,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_InvoiceLine (
    SAT_InvoiceLineID SERIAL PRIMARY KEY,
    InvoiceLineID INT REFERENCES HUB_InvoiceLine(InvoiceLineID),
    Quantity INT,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Track (
    SAT_TrackID SERIAL PRIMARY KEY,
    TrackID INT REFERENCES HUB_Tracks(TrackID),
    UnitPrice NUMERIC(10,2),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_Title (
    SAT_TitleID SERIAL PRIMARY KEY,
    TitleID INT REFERENCES HUB_Titles(TitleID),
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);

CREATE TABLE SAT_EmployeeTitle (
    SAT_EmployeeTitleID SERIAL PRIMARY KEY,
    EmployeeTitleID INT REFERENCES LINK_EmployeeTitle(EmployeeTitleID),
    AppointmentDate DATE,
    LoadDate TIMESTAMP NOT NULL,
    Source VARCHAR(255)
);