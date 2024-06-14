CREATE TABLE IF NOT EXISTS Customer (
    CustomerID INT NOT NULL, 
    PersonID INT, 
    StoreID INT,  
    TerritoryID INT,
    AccountNumber STRING, 
    rowguid STRING, -- Use STRING for UUID
    ModifiedDate TIMESTAMP
);