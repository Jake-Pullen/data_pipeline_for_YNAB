# ERD for the Finance DataWarehouse

```mermaid
erDiagram
    ACCOUNTS {
        int account_id
        string account_name
        string account_type
        boolean on_budget
        boolean closed
        text note
        decimal balance
        decimal cleared_balance
        decimal uncleared_balance
        boolean deleted
    }
    
    CATEGORIES {
        int category_id
        string category_name
        string category_group_name
        boolean hidden
        text note
        decimal budgeted
        decimal activity
        decimal balance
        boolean deleted
    }
    
    PAYEES {
        int payee_id
        string payee_name
        boolean deleted
    }
    
    DATES {
        int date_id
        string date
        int year
        int month
        int day
    }
    
    TRANSACTIONS {
        int transaction_id
        int account_id
        int category_id
        int payee_id
        int date_id
        decimal amount
        boolean cleared
        boolean approved
        boolean deleted
    }
    
    SCHEDULED_TRANSACTIONS {
        int scheduled_transaction_id
        int account_id
        int category_id
        int payee_id
        int date_id
        decimal amount
        string frequency
        boolean deleted
    }
    
    TRANSACTIONS ||--o{ ACCOUNTS : "belongs to"
    TRANSACTIONS ||--o{ CATEGORIES : "belongs to"
    TRANSACTIONS ||--o{ PAYEES : "belongs to"
    TRANSACTIONS ||--o{ DATES : "occurred on"
    SCHEDULED_TRANSACTIONS ||--o{ ACCOUNTS : "belongs to"
    SCHEDULED_TRANSACTIONS ||--o{ CATEGORIES : "belongs to"
    SCHEDULED_TRANSACTIONS ||--o{ PAYEES : "belongs to"
    SCHEDULED_TRANSACTIONS ||--o{ DATES : "scheduled on"
```

