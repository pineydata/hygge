#!/usr/bin/env python3
"""
Generate sample parquet data for the Entity-First approach testing.

Creates multiple parquet files with different sizes:
- Small files (1K-5K rows)
- Medium files (10K-50K rows)
- Large files (100K+ rows)
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


def generate_ip_address() -> str:
    """Generate a random IP address."""
    octets = [str(random.randint(1, 255)) for _ in range(4)]
    return ".".join(octets)


def generate_accounts_data(num_rows: int) -> pl.DataFrame:
    """Generate sample accounts data."""
    # Generate realistic account data
    account_types = ["Enterprise", "SMB", "Startup", "Non-Profit", "Government"]
    industries = [
        "Technology",
        "Healthcare",
        "Finance",
        "Retail",
        "Manufacturing",
        "Education",
        "Energy",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "Name": [f"Account_{i:06d}" for i in range(1, num_rows + 1)],
        "Type": [random.choice(account_types) for _ in range(num_rows)],
        "Industry": [random.choice(industries) for _ in range(num_rows)],
        "AnnualRevenue": [random.randint(10000, 10000000) for _ in range(num_rows)],
        "NumberOfEmployees": [random.randint(1, 5000) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_contacts_data(num_rows: int) -> pl.DataFrame:
    """Generate sample contacts data."""
    first_names = [
        "John",
        "Jane",
        "Mike",
        "Sarah",
        "David",
        "Lisa",
        "Chris",
        "Amy",
        "Mark",
        "Emma",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "FirstName": [random.choice(first_names) for _ in range(num_rows)],
        "LastName": [random.choice(last_names) for _ in range(num_rows)],
        "Email": [f"contact_{i:06d}@example.com" for i in range(1, num_rows + 1)],
        "Phone": [
            f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            for _ in range(num_rows)
        ],
        "AccountId": [random.randint(1, 1000) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_opportunities_data(num_rows: int) -> pl.DataFrame:
    """Generate sample opportunities data."""
    stages = [
        "Prospecting",
        "Qualification",
        "Proposal",
        "Negotiation",
        "Closed Won",
        "Closed Lost",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "Name": [f"Opportunity_{i:06d}" for i in range(1, num_rows + 1)],
        "Stage": [random.choice(stages) for _ in range(num_rows)],
        "Amount": [random.randint(1000, 1000000) for _ in range(num_rows)],
        "Probability": [random.randint(10, 100) for _ in range(num_rows)],
        "AccountId": [random.randint(1, 1000) for _ in range(num_rows)],
        "ContactId": [random.randint(1, 2000) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_leads_data(num_rows: int) -> pl.DataFrame:
    """Generate sample leads data."""
    statuses = ["New", "Working", "Contacted", "Qualified", "Unqualified", "Converted"]
    sources = [
        "Website",
        "Phone",
        "Email",
        "Referral",
        "Social Media",
        "Trade Show",
        "Cold Call",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "FirstName": [f"Lead_{i:06d}_First" for i in range(1, num_rows + 1)],
        "LastName": [f"Lead_{i:06d}_Last" for i in range(1, num_rows + 1)],
        "Email": [f"lead_{i:06d}@example.com" for i in range(1, num_rows + 1)],
        "Company": [f"Company_{i:06d}" for i in range(1, num_rows + 1)],
        "Status": [random.choice(statuses) for _ in range(num_rows)],
        "Source": [random.choice(sources) for _ in range(num_rows)],
        "Rating": [random.randint(1, 5) for _ in range(num_rows)],
        "AnnualRevenue": [random.randint(10000, 5000000) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_cases_data(num_rows: int) -> pl.DataFrame:
    """Generate sample cases data."""
    statuses = ["New", "Working", "Escalated", "Closed", "On Hold"]
    priorities = ["Low", "Medium", "High", "Critical"]
    types = ["Question", "Problem", "Feature Request", "Bug Report", "Complaint"]

    data = {
        "Id": range(1, num_rows + 1),
        "CaseNumber": [f"CASE-{i:06d}" for i in range(1, num_rows + 1)],
        "Subject": [f"Case Subject {i:06d}" for i in range(1, num_rows + 1)],
        "Description": [
            f"Case description for case {i:06d}" for i in range(1, num_rows + 1)
        ],
        "Status": [random.choice(statuses) for _ in range(num_rows)],
        "Priority": [random.choice(priorities) for _ in range(num_rows)],
        "Type": [random.choice(types) for _ in range(num_rows)],
        "AccountId": [random.randint(1, 1000) for _ in range(num_rows)],
        "ContactId": [random.randint(1, 2000) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_products_data(num_rows: int) -> pl.DataFrame:
    """Generate sample products data."""
    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
        "Beauty",
        "Automotive",
    ]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

    data = {
        "Id": range(1, num_rows + 1),
        "Name": [f"Product_{i:06d}" for i in range(1, num_rows + 1)],
        "Description": [
            f"Description for product {i:06d}" for i in range(1, num_rows + 1)
        ],
        "Category": [random.choice(categories) for _ in range(num_rows)],
        "Brand": [random.choice(brands) for _ in range(num_rows)],
        "Price": [random.uniform(10.0, 1000.0) for _ in range(num_rows)],
        "StockQuantity": [random.randint(0, 1000) for _ in range(num_rows)],
        "IsActive": [random.choice([True, False]) for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_categories_data(num_rows: int) -> pl.DataFrame:
    """Generate sample categories data."""
    data = {
        "Id": range(1, num_rows + 1),
        "Name": [f"Category_{i:06d}" for i in range(1, num_rows + 1)],
        "Description": [
            f"Description for category {i:06d}" for i in range(1, num_rows + 1)
        ],
        "ParentCategoryId": [
            random.randint(1, num_rows) if random.random() > 0.7 else None
            for _ in range(num_rows)
        ],
        "IsActive": [True] * num_rows,
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_regions_data(num_rows: int) -> pl.DataFrame:
    """Generate sample regions data."""
    countries = [
        "USA",
        "Canada",
        "Mexico",
        "UK",
        "Germany",
        "France",
        "Japan",
        "Australia",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "Name": [f"Region_{i:06d}" for i in range(1, num_rows + 1)],
        "Country": [random.choice(countries) for _ in range(num_rows)],
        "Population": [random.randint(10000, 10000000) for _ in range(num_rows)],
        "Area": [random.uniform(100.0, 100000.0) for _ in range(num_rows)],
        "IsActive": [True] * num_rows,
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_orders_data(num_rows: int) -> pl.DataFrame:
    """Generate sample orders data."""
    statuses = [
        "Pending",
        "Processing",
        "Shipped",
        "Delivered",
        "Cancelled",
        "Returned",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "OrderNumber": [f"ORD-{i:08d}" for i in range(1, num_rows + 1)],
        "CustomerId": [random.randint(1, 50000) for _ in range(num_rows)],
        "Status": [random.choice(statuses) for _ in range(num_rows)],
        "TotalAmount": [random.uniform(10.0, 5000.0) for _ in range(num_rows)],
        "OrderDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "ShippedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            if random.random() > 0.3
            else None
            for _ in range(num_rows)
        ],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_customers_data(num_rows: int) -> pl.DataFrame:
    """Generate sample customers data."""
    first_names = [
        "John",
        "Jane",
        "Mike",
        "Sarah",
        "David",
        "Lisa",
        "Chris",
        "Amy",
        "Mark",
        "Emma",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "FirstName": [random.choice(first_names) for _ in range(num_rows)],
        "LastName": [random.choice(last_names) for _ in range(num_rows)],
        "Email": [f"customer_{i:06d}@example.com" for i in range(1, num_rows + 1)],
        "Phone": [
            f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            for _ in range(num_rows)
        ],
        "Address": [f"Address {i:06d}" for i in range(1, num_rows + 1)],
        "City": [f"City_{i:06d}" for i in range(1, num_rows + 1)],
        "State": [f"State_{i:06d}" for i in range(1, num_rows + 1)],
        "ZipCode": [f"{random.randint(10000, 99999)}" for _ in range(num_rows)],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_transactions_data(num_rows: int) -> pl.DataFrame:
    """Generate sample transactions data."""
    types = ["Purchase", "Refund", "Payment", "Transfer", "Fee", "Interest"]

    data = {
        "Id": range(1, num_rows + 1),
        "TransactionId": [f"TXN-{i:010d}" for i in range(1, num_rows + 1)],
        "AccountId": [random.randint(1, 100000) for _ in range(num_rows)],
        "Type": [random.choice(types) for _ in range(num_rows)],
        "Amount": [random.uniform(-1000.0, 10000.0) for _ in range(num_rows)],
        "Description": [
            f"Transaction description {i:06d}" for i in range(1, num_rows + 1)
        ],
        "TransactionDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_events_data(num_rows: int) -> pl.DataFrame:
    """Generate sample events data."""
    event_types = [
        "Login",
        "Logout",
        "Purchase",
        "View",
        "Click",
        "Download",
        "Upload",
        "Error",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "EventId": [f"EVT-{i:010d}" for i in range(1, num_rows + 1)],
        "UserId": [random.randint(1, 100000) for _ in range(num_rows)],
        "EventType": [random.choice(event_types) for _ in range(num_rows)],
        "EventData": [f"Event data for {i:06d}" for i in range(1, num_rows + 1)],
        "IpAddress": [generate_ip_address() for _ in range(num_rows)],
        "UserAgent": [f"UserAgent_{i:06d}" for i in range(1, num_rows + 1)],
        "EventDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_logs_data(num_rows: int) -> pl.DataFrame:
    """Generate sample logs data."""
    log_levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    modules = ["Auth", "Payment", "Inventory", "Shipping", "Customer", "Order"]

    data = {
        "Id": range(1, num_rows + 1),
        "LogId": [f"LOG-{i:010d}" for i in range(1, num_rows + 1)],
        "Level": [random.choice(log_levels) for _ in range(num_rows)],
        "Module": [random.choice(modules) for _ in range(num_rows)],
        "Message": [f"Log message {i:06d}" for i in range(1, num_rows + 1)],
        "Exception": [
            f"Exception details {i:06d}" if random.random() > 0.8 else None
            for i in range(1, num_rows + 1)
        ],
        "UserId": [
            random.randint(1, 100000) if random.random() > 0.5 else None
            for _ in range(num_rows)
        ],
        "SessionId": [f"SESSION-{i:08d}" for i in range(1, num_rows + 1)],
        "LogDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def generate_inventory_data(num_rows: int) -> pl.DataFrame:
    """Generate sample inventory data."""
    locations = [
        "Warehouse A",
        "Warehouse B",
        "Store 1",
        "Store 2",
        "Store 3",
        "Online",
    ]

    data = {
        "Id": range(1, num_rows + 1),
        "ProductId": [random.randint(1, 100000) for _ in range(num_rows)],
        "Location": [random.choice(locations) for _ in range(num_rows)],
        "Quantity": [random.randint(0, 10000) for _ in range(num_rows)],
        "ReservedQuantity": [random.randint(0, 1000) for _ in range(num_rows)],
        "ReorderLevel": [random.randint(10, 500) for _ in range(num_rows)],
        "LastRestocked": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "CreatedDate": [
            datetime.now() - timedelta(days=random.randint(1, 3650))
            for _ in range(num_rows)
        ],
        "LastModifiedDate": [
            datetime.now() - timedelta(days=random.randint(1, 30))
            for _ in range(num_rows)
        ],
        "IsDeleted": [False] * num_rows,
    }

    return pl.DataFrame(data)


def main():
    """Generate sample data for all entities."""
    print("🏠 Generating sample data for Entity-First approach...")

    # Define data generation plan - repo-friendly 3-table sample
    data_plan = [
        # Small table (5K rows) - quick demo
        ("opportunities", 5000),
        # Medium table (50K rows) - typical business table
        ("accounts", 50000),
        # Large table (150K rows) - demonstrates CCI batching
        ("contacts", 150000),
    ]

    # Create base directory
    base_dir = Path("data/source")
    base_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0

    for entity, num_rows in data_plan:
        print(f"\n📊 Generating {entity} data: {num_rows:,} rows")

        # Create entity directory
        entity_dir = base_dir / entity
        entity_dir.mkdir(parents=True, exist_ok=True)

        # Generate data based on entity type
        if entity == "accounts":
            df = generate_accounts_data(num_rows)
        elif entity == "contacts":
            df = generate_contacts_data(num_rows)
        elif entity == "opportunities":
            df = generate_opportunities_data(num_rows)
        else:
            print(f"⚠️  Unknown entity type: {entity}")
            continue

        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y_%m")
        filename = f"{entity}_{timestamp}.parquet"
        filepath = entity_dir / filename

        # Write parquet file
        df.write_parquet(filepath)

        # Show file info
        file_size = filepath.stat().st_size
        print(f"   ✅ Created: {filename}")
        print(f"   📄 Size: {file_size:,} bytes")
        print(f"   📊 Rows: {len(df):,}")

        total_rows += len(df)

    print("\n🎉 Data generation complete!")
    print(f"📊 Total rows generated: {total_rows:,}")
    print(f"📁 Data location: {base_dir}")

    print("\n📊 Data Summary:")
    print("   🔹 Small table: opportunities (5K rows) - quick demo")
    print("   🔹 Medium table: accounts (50K rows) - typical business table")
    print("   🔹 Large table: contacts (150K rows) - demonstrates CCI batching")
    print("   🔹 Total entities: 3 (repo-friendly sample)")

    # Show directory structure
    print("\n📂 Directory structure:")
    for entity_dir in base_dir.iterdir():
        if entity_dir.is_dir():
            files = list(entity_dir.glob("*.parquet"))
            print(f"   {entity_dir.name}/ ({len(files)} files)")
            for file in files:
                size = file.stat().st_size
                print(f"      📄 {file.name} ({size:,} bytes)")


if __name__ == "__main__":
    main()
