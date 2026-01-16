# Explanation of `group_by` Parameter (Lines 78-81)

## Purpose

The `group_by` parameter specifies which **categorical columns** to use for **counting/aggregation**. It tells the system: "Count how many rows exist for each unique value in these columns."

---

## Current Code (Lines 78-81)

```python
analyze_config = {
    'columns': compound_col_name,  # Columns to analyze
    'group_by': []  # Can be extended if needed
}
```

**Currently:** `group_by` is an **empty list** `[]`, which means **no grouping/counting is performed**.

---

## How It Works

### When `group_by` is Empty `[]`:

```python
group_by = []  # No grouping
```

**What happens:**
- ✅ Max/min values are still calculated
- ✅ Unique values are still tracked
- ❌ **No counting by groups** (no aggregation)

**Example:**
```python
# Input data
product  | category | price
Laptop   | Tech     | 1200
Phone    | Tech     | 800
Tablet   | Tech     | 600
Shirt    | Clothing | 50

# With group_by = []
Results:
  - max_values['price'] = {'product': 'Laptop', 'category': 'Tech', 'price': 1200}
  - unique_values['category'] = {'Tech', 'Clothing'}
  - group_counts = {}  # Empty! No counts calculated
```

---

### When `group_by` Has Columns `['category']`:

```python
group_by = ['category']  # Count by category
```

**What happens:**
- ✅ Max/min values are calculated
- ✅ Unique values are tracked
- ✅ **Counts are calculated grouped by category**

**Example:**
```python
# Input data
product  | category | price
Laptop   | Tech     | 1200
Phone    | Tech     | 800
Tablet   | Tech     | 600
Shirt    | Clothing | 50
Pants    | Clothing | 80

# With group_by = ['category']
Results:
  - max_values['price'] = {'product': 'Laptop', 'category': 'Tech', 'price': 1200}
  - unique_values['category'] = {'Tech', 'Clothing'}
  - group_counts['category'] = {
      'Tech': 3,        # 3 products in Tech category
      'Clothing': 2     # 2 products in Clothing category
  }
```

---

## Visual Example

### Data:
```
┌─────────┬──────────┬───────┐
│ product │ category │ price │
├─────────┼──────────┼───────┤
│ Laptop  │ Tech     │ 1200  │
│ Phone   │ Tech     │ 800   │
│ Tablet  │ Tech     │ 600   │
│ Shirt   │ Clothing │ 50     │
│ Pants   │ Clothing │ 80     │
└─────────┴──────────┴───────┘
```

### With `group_by = []`:
```
Results:
  group_counts = {}  # No counts
```

### With `group_by = ['category']`:
```
Results:
  group_counts = {
    'category': {
      'Tech': 3,      # Count of Tech products
      'Clothing': 2   # Count of Clothing products
    }
  }
```

---

## Code Flow

### Line 422: Extract group_by columns
```python
group_by_cols = analyze_config.get('group_by', [])
# Result: ['category'] or []
```

### Lines 452-455: Process group_by columns
```python
# Count by group columns using PyArrow group_by
for group_col in group_by_cols:  # Loop through ['category']
    if group_col in chunk.column_names:
        self._update_group_counts(chunk, group_col, comp_col)
```

### What `_update_group_counts()` does (Lines 507-538):
```python
# Uses PyArrow's group_by for efficient aggregation
grouped = chunk_with_count.group_by(group_col).aggregate([
    ('_count', 'sum')
])

# Result stored in:
comp_col.group_counts['category'] = {
    'Tech': 3,
    'Clothing': 2
}
```

---

## Real-World Use Cases

### Use Case 1: Count Products by Category
```python
analyze_config = {
    'columns': ['product', 'category', 'price'],
    'group_by': ['category']  # Count how many products per category
}

# Result:
group_counts = {
    'category': {
        'Tech': 150,
        'Clothing': 200,
        'Food': 75
    }
}
```

### Use Case 2: Count by Multiple Columns
```python
analyze_config = {
    'columns': ['product', 'category', 'region', 'price'],
    'group_by': ['category', 'region']  # Count by category AND region
}

# Result:
group_counts = {
    'category': {
        'Tech': 150,
        'Clothing': 200
    },
    'region': {
        'North': 100,
        'South': 250
    }
}
```

### Use Case 3: No Grouping (Current Implementation)
```python
analyze_config = {
    'columns': ['product', 'category', 'price'],
    'group_by': []  # No grouping - just max/min and unique values
}

# Result:
group_counts = {}  # Empty - no counts calculated
```

---

## Why It's Currently Empty `[]`

Looking at line 80, it's set to an empty list with a comment: `# Can be extended if needed`

**Reasons:**
1. **Optional Feature:** Grouping is optional - you might only want max/min values
2. **Flexibility:** Users can add columns later if they need counting
3. **Performance:** Skipping grouping saves processing time if not needed

---

## How to Use It

### Option 1: Keep Empty (Current)
```python
analyze_config = {
    'columns': compound_col_name,
    'group_by': []  # No grouping
}
```
**Result:** Only max/min and unique values, no counts

### Option 2: Add Category Grouping
```python
analyze_config = {
    'columns': compound_col_name,
    'group_by': ['category']  # Count by category
}
```
**Result:** Counts how many rows per category

### Option 3: Add Multiple Groupings
```python
analyze_config = {
    'columns': compound_col_name,
    'group_by': ['category', 'region']  # Count by both
}
```
**Result:** Counts for each column separately

---

## Summary

| `group_by` Value | What It Does |
|-----------------|--------------|
| `[]` (empty) | No grouping/counting. Only calculates max/min and unique values |
| `['category']` | Counts rows grouped by category (e.g., Tech: 3, Clothing: 2) |
| `['category', 'region']` | Counts rows for each column separately |

**Current State:** Empty list `[]` = **No grouping/counting is performed**

**To Enable:** Add column names to the list, e.g., `['category']` to count by category
