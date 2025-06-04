import pandas as pd
import random

# Sample data pools
manufacturers = ['Carrier', 'Trane', 'Lennox', 'Goodman', 'Rheem', 'York']
models = [
    'Infinity 98', 'XR16', 'EL296V', 'GSXC18', 'Prestige Series', 'Affinity Series',
    'Infinity 80', 'XR13', 'SL280V', 'GSX14', 'Classic Series', 'LX Series'
]

# Generate 100 sample records
data = []
for i in range(100):
    tenant_id = f"tenant_{random.randint(1, 30):03d}"  # Some tenants will repeat
    manufacturer = random.choice(manufacturers)
    model = random.choice(models)
    data.append({'Tenant_ID': tenant_id, 'Manufacturer': manufacturer, 'Model': model})

# Create DataFrame and save to CSV
df = pd.DataFrame(data)
df.to_csv("tenant_equipment_data_sample.csv", index=False)

