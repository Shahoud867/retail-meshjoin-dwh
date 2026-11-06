# Data note

**Source.** Chen, D., Sain, S. L., and Guo, K. *Online Retail*. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/352/online+retail

**Downloaded.** 3 November 2026. File `Online Retail.xlsx`, SHA-256 `43465a06f2ccf7c8b5bd2892bc7defb52f97487934fe93b16ae4c3936424676d`. It covers 1 December 2010 to 9 December 2011.

**Files here.** All are produced by the program from that one file, with no hand edits.

| File | Rows | Columns |
|---|---|---|
| `online_sales_stream.csv` | 278,740 | transaction_id, customer_id, product_id, quantity, unit_price, event_time, sales_mode |
| `customer_master_data.csv` | 4,338 (9 partitions) | customer_id, gender, age_group, occupation, city_category, stay_in_current_city_years, marital_status |
| `product_master_data.csv` | 3,665 (8 partitions) | product_id, product_name, product_category, supplier_id, supplier_name |

The other 119,144 transactions are periodic. They live in the staging table `periodic_sales_data`, in 48 groups: 8 of 2,483 and 40 of 2,482.

**Cleaning counts.** Before 541,909, after 397,884. A transaction is kept only when quantity > 0, unit price > 0, customer id present, and `transaction_id` does not start with C. Failures per test, which overlap: cancellations 9,288; quantity 10,624; price 2,517; customer 135,080. No other rows are removed, so 5,194 exact duplicates stay.

**Rules.** `stable(text, n)` is the MD5 of the UTF-8 text, first 8 hex characters as an integer, modulo n.

- Sales mode: `stable(transaction_id, 10)`: 0–6 online, 7–9 periodic.
- Store: online is store 1; otherwise `2 + stable(transaction_id, 4)`.
- Customer: gender `stable(id, 2)`; age `stable(id + "age", 7)`; occupation `stable(id + "occ", 10)`; city category `stable(id + country, 3)`, using the country on the customer's first cleaned row; stay `stable(id + "stay", 5)`; marital status `stable(id + "mar", 2)`.
- Product: category `stable(id + "cat", 10)`; supplier `stable(id, 5)`; name is the longest Description after trimming edge spaces, the first in file order winning a tie, or `UNKNOWN` if empty.

Worked example: `536365` has MD5 starting `3e6d49ce`, which is 1047349710. Remainder 0 means online, store 1.
