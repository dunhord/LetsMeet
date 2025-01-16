SELECT *
FROM 
    users u
LEFT JOIN 
    addresses a ON u.address_id = a.address_id;