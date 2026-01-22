## Explore crm_cust_info_raw
# View Raw data 
df_crm_cust_info = spark.table("dev_project.bronze.crm_cust_info_raw")
df_crm_cust_info.show()

# count the number of rows
df_crm_cust_info_count = df_crm_cust_info.count()
print(f"Total Number of rows in prd info : {df_crm_cust_info_count}")

print("---------------------------------------------------------------------------------\n")

# Duplicates in cst_id
print("Duplicates in cst_id")
duplicates_df_crm_cust_info = df_crm_cust_info.groupBy(df_crm_cust_info.cst_id).count().filter("count > 1")
display(duplicates_df_crm_cust_info)

print("---------------------------------------------------------------------------------\n")

df_crm_cust_info.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_crm_cust_info.columns:
    print(c)
    df_crm_cust_info.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")
for c in df_crm_cust_info.columns:
    print(c)
    df_crm_cust_info.filter(col(c).isNull()).show()
    crm_cust_info_null_count = df_crm_cust_info.filter(col(c).isNull()).count()
    print(f"{c} null count: {crm_cust_info_null_count}\n")



## Explore crm_prd_cust_info_raw

# View cust_prd Raw data 
df_prd_info = spark.table("dev_project.bronze.crm_prd_cust_info_raw")
df_prd_info.show()

# count the number of rows
df_prd_info_count = df_prd_info.count()
print(f"Total Number of rows in prd info : {df_prd_info_count}")
print("---------------------------------------------------------------------------------\n")

# Duplicates in prd_id
print("Duplicates in prd_id")
duplicates_df_prd_info = df_prd_info.groupBy(df_prd_info.prd_id).count().filter("count > 1")
display(duplicates_df_prd_info)

print("---------------------------------------------------------------------------------\n")

df_prd_info.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_prd_info.columns:
    print(c)
    df_prd_info.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

df_prd_info.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")

for c in df_prd_info.columns:
    print(c)
    df_prd_info.filter(col(c).isNull()).show()
    prd_null_count = df_prd_info.filter(col(c).isNull()).count()
    print(f"{c} null count: {prd_null_count}\n")

print("---------------------------------------------------------------------------------\n")


## Explore crm_sales_details Raw

# View sales_details Raw data 
df_sales_details = spark.table("dev_project.bronze.crm_sales_details_info_raw")
display(df_sales_details)

# count the number of rows
df_sales_details_count = df_sales_details.count()
print(f"Total Number of rows in  sales_details : {df_sales_details_count}")
print("---------------------------------------------------------------------------------\n")

# Duplicates in sls_ord_num
print("Duplicates in sls_d_ord_num")
duplicates_df_sales_details = df_sales_details.groupBy(df_sales_details.sls_ord_num).count().filter("count > 1")
display(duplicates_df_sales_details)

print("---------------------------------------------------------------------------------\n")

df_sales_details.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_sales_details.columns:
    print(c)
    df_sales_details.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

df_sales_details.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")

for c in df_sales_details.columns:
    print(c)
    df_sales_details.filter(col(c).isNull()).show()
    sales_null_count =df_sales_details.filter(col(c).isNull()).count()
    print(f"{c} null count: {sales_null_count}\n")

print("---------------------------------------------------------------------------------\n")

## expore erp_cust_az12 raw

# View Erp Raw data 
df_erp_cust_az12 = spark.table("dev_project.bronze.erp_cust_az12_raw")
display(df_erp_cust_az12)

# count the number of rows
df_erp_cust_az12_count = df_erp_cust_az12.count()
print(f"Total Number of rows in  erp_cust_az12 : {df_erp_cust_az12_count}")
print("---------------------------------------------------------------------------------\n")

# Duplicates in erp_cust_az12
print("Duplicates in erp_cust_az12")
duplicates_df_erp_cust_az12= df_erp_cust_az12.groupBy(df_erp_cust_az12.CID).count().filter("count > 1")
display(duplicates_df_erp_cust_az12)

print("---------------------------------------------------------------------------------\n")

df_erp_cust_az12.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_erp_cust_az12.columns:
    print(c)
    df_erp_cust_az12.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

df_erp_cust_az12.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")

for c in df_erp_cust_az12.columns:
    print(c)
    df_erp_cust_az12.filter(col(c).isNull()).show()
    erp_cust_az12_null_count =df_erp_cust_az12.filter(col(c).isNull()).count()
    print(f"{c} null count: {erp_cust_az12_null_count}\n")

print("---------------------------------------------------------------------------------\n")

## Explore Erp_loc_a101_raw

# View Erp Raw data 
df_erp_loc_a101 = spark.table("dev_project.bronze.erp_loc_a101_raw")
display(df_erp_loc_a101)

# count the number of rows
df_erp_loc_a101_count = df_erp_loc_a101.count()
print(f"Total Number of rows in  erp_cust_az12 : {df_erp_loc_a101_count}")
print("---------------------------------------------------------------------------------\n")

# Duplicates in erp_loc_a101
print("Duplicates in erp_loc_a101")
duplicates_df_erp_loc_a101= df_erp_loc_a101.groupBy(df_erp_loc_a101.CID).count().filter("count > 1")
display(duplicates_df_erp_loc_a101)

print("---------------------------------------------------------------------------------\n")

df_erp_loc_a101.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_erp_loc_a101.columns:
    print(c)
    df_erp_loc_a101.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

df_erp_loc_a101.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")

for c in df_erp_loc_a101.columns:
    print(c)
    df_erp_loc_a101.filter(col(c).isNull()).show()
    erp_loc_a101_null_count =df_erp_loc_a101.filter(col(c).isNull()).count()
    print(f"{c} null count: {erp_loc_a101_null_count}\n")

print("---------------------------------------------------------------------------------\n")

## Explore erp_px_cat_g1v2_raw

# View Erp Raw data 
df_erp_px_cat_g1v2 = spark.table("dev_project.bronze.erp_px_cat_g1v2_raw")
display(df_erp_px_cat_g1v2)

# count the number of rows
df_erp_px_cat_g1v2_count = df_erp_px_cat_g1v2.count()
print(f"Total Number of rows in  erp_px_cat_g1v2: {df_erp_px_cat_g1v2_count}")
print("---------------------------------------------------------------------------------\n")

# Duplicates in erp_px_cat_g1v2
print("Duplicates in erp_px_cat_g1v2")
duplicates_df_erp_px_cat_g1v2 = df_erp_px_cat_g1v2.groupBy(df_erp_px_cat_g1v2.ID).count().filter("count > 1")
display(duplicates_df_erp_px_cat_g1v2)

print("---------------------------------------------------------------------------------\n")

df_erp_px_cat_g1v2.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation of string values : check extra spaces 
print("validation of string values : check extra spaces ")
for c in df_erp_px_cat_g1v2.columns:
    print(c)
    df_erp_px_cat_g1v2.filter(length(col(c)) != length(trim(col(c)))).show()

print("---------------------------------------------------------------------------------\n")

df_erp_px_cat_g1v2.printSchema()

print("---------------------------------------------------------------------------------\n")

# validation null Values
print("validation null Values")

for c in df_erp_px_cat_g1v2.columns:
    print(c)
    df_erp_px_cat_g1v2.filter(col(c).isNull()).show()
    erp_px_cat_g1v2_null_count =df_erp_px_cat_g1v2.filter(col(c).isNull()).count()
    print(f"{c} null count: {erp_px_cat_g1v2_null_count}\n")

print("---------------------------------------------------------------------------------\n")

