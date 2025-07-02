WITH chain_TEMP AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME desc) AS rn
    FROM 
        REECO.MONGO.BUYERSERVICE_CHAINS
)

select
    _id as chain_id,
    NAME as chain_name,
    CREATEDATETIME as CREATE_DATETIME,
    ISDISABLED as IS_DISABLED,
    ISDEMO AS IS_DEMO
    
 from  chain_TEMP
 where rn = 1
 
 
