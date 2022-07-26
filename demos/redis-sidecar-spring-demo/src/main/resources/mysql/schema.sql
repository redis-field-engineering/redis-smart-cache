
CREATE TABLE IF NOT EXISTS products (
  productCode INT NOT NULL,
  productName VARCHAR(70) NOT NULL,
  quantityInStock INT NOT NULL,
  MSRP DECIMAL NOT NULL,
  buyPrice DECIMAL NOT NULL,
  productVendor VARCHAR(50) NOT NULL,
  productLine VARCHAR(50) NOT NULL,
  productScale VARCHAR(10) NOT NULL,
  productDescription TEXT NOT NULL,
  PRIMARY KEY (productCode)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS customers (
  customerNumber INT NOT NULL,
  customerName VARCHAR(50) NOT NULL,
  contactLastName VARCHAR(50) NOT NULL,
  contactFirstName VARCHAR(50) NOT NULL,
  phone VARCHAR(50) NOT NULL,
  addressLine1 VARCHAR(50) NOT NULL,
  addressLine2 VARCHAR(50) DEFAULT NULL,
  postalCode VARCHAR(15) DEFAULT NULL,
  country VARCHAR(200) NOT NULL,
  PRIMARY KEY (customerNumber)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS orders (
  orderNumber INT NOT NULL,
  orderDate DATE NOT NULL,
  requiredDate DATE NOT NULL,
  shippedDate DATE DEFAULT NULL,
  status VARCHAR(15) NOT NULL,
  customerNumber INT NOT NULL,
  PRIMARY KEY (orderNumber),
  KEY customerNumber (customerNumber),
  CONSTRAINT orders_ibfk_1 FOREIGN KEY (customerNumber) REFERENCES customers (customerNumber)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS orderdetails (
  orderNumber INT NOT NULL,
  productCode INT NOT NULL,
  quantityOrdered INT NOT NULL,
  orderLineNumber INT NOT NULL,
  priceEach DECIMAL NOT NULL,
  PRIMARY KEY (orderNumber,productCode),
  KEY productCode (productCode),
  CONSTRAINT orderdetails_ibfk_1 FOREIGN KEY (orderNumber) REFERENCES orders (orderNumber),
  CONSTRAINT orderdetails_ibfk_2 FOREIGN KEY (productCode) REFERENCES products (productCode)
) ENGINE=INNODB;