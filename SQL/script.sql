

CREATE TABLE IF NOT EXISTS property (
            property_id INT PRIMARY KEY,
            Property_Title VARCHAR(255),
            Address VARCHAR(255),
            Market VARCHAR(50),
            Flood VARCHAR(50),
            Street_Address VARCHAR(255),
            City VARCHAR(100),
            State VARCHAR(10),
            Zip VARCHAR(10),
            Property_Type VARCHAR(50),
            Highway VARCHAR(50),
            Train VARCHAR(50),
            Tax_Rate FLOAT,
            SQFT_Basement FLOAT,
            HTW VARCHAR(10),
            Pool VARCHAR(10),
            Commercial VARCHAR(10),
            Water VARCHAR(50),
            Sewage VARCHAR(50),
            Year_Built INT,
            SQFT_MU FLOAT,
            SQFT_Total FLOAT,
            Parking VARCHAR(50),
            Bed INT,
            Bath INT,
            BasementYesNo VARCHAR(10),
            Layout VARCHAR(50),
            Rent_Restricted VARCHAR(10),
            Neighborhood_Rating FLOAT,
            Latitude FLOAT,
            Longitude FLOAT,
            Subdivision VARCHAR(100),
            School_Average FLOAT
        );


        CREATE TABLE IF NOT EXISTS leads (
            lead_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Reviewed_Status VARCHAR(50),
            Most_Recent_Status VARCHAR(50),
            Source VARCHAR(50),
            Occupancy VARCHAR(10),
            Net_Yield FLOAT,
            IRR FLOAT,
            Selling_Reason VARCHAR(255),
            Seller_Retained_Broker VARCHAR(255),
            Final_Reviewer VARCHAR(100),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );


        CREATE TABLE IF NOT EXISTS valuation (
            valuation_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            List_Price FLOAT,
            Previous_Rent FLOAT,
            ARV FLOAT,
            Rent_Zestimate FLOAT,
            Low_FMR FLOAT,
            High_FMR FLOAT,
            Zestimate FLOAT,
            Expected_Rent FLOAT,
            Redfin_Value FLOAT,
            avg_rent FLOAT,
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );


        CREATE TABLE IF NOT EXISTS hoa (
            hoa_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            HOA FLOAT,
            HOA_Flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );


        CREATE TABLE IF NOT EXISTS rehab (
            rehab_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Underwriting_Rehab FLOAT,
            Rehab_Calculation FLOAT,
            Paint VARCHAR(10),
            Flooring_Flag VARCHAR(10),
            Foundation_Flag VARCHAR(10),
            Roof_Flag VARCHAR(10),
            HVAC_Flag VARCHAR(10),
            Kitchen_Flag VARCHAR(10),
            Bathroom_Flag VARCHAR(10),
            Appliances_Flag VARCHAR(10),
            Windows_Flag VARCHAR(10),
            Landscaping_Flag VARCHAR(10),
            Trashout_Flag VARCHAR(10),
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );

        CREATE TABLE IF NOT EXISTS taxes (
            tax_id INT AUTO_INCREMENT PRIMARY KEY,
            property_id INT,
            Taxes FLOAT,
            FOREIGN KEY (property_id) REFERENCES property(property_id)
        );