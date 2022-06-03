import psycopg2


def get_data_from_database(events):
    """
    Connects and get data from database.
    """
    conn = psycopg2.connect(
        database=events['database'],
        host=events['host'],
        user=events['username'],
        password=events['password'],
    )
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT *,
        EXTRACT(YEAR FROM AGE(CURRENT_TIMESTAMP, customers.date_of_birth)) as age
        from transactions
        INNER JOIN
        customers
        ON
        transactions.customer_id=customers.customer_id
        where (transaction_date::timestamp)::date = '{events['date']}'
        """
    )
    results = cursor.fetchall()

    return results


def calculate_savings(events, context=None):
    """
    Calculates the savings age wise for the data retrieved.
    """
    results = get_data_from_database(events)
    response = dict()
    for i, row in enumerate(results):
        amount = row[3]
        if row[2] == 'DEBIT':
            amount = -amount
        try:
            response[int(row[9])].append(amount)
        except KeyError:
            response.update({int(str(row[9])): [amount]})

    for key, value in response.items():
        response[key] = sum(response[key]) / len(response[key])

    return response


if __name__ == '__main__':
    events = {
        "database": "test",
        "port": "5432",
        "host": "localhost",
        "username": "postgres",
        "password": "psql",
        "date": "2022-05-31",
    }
    try:
        results = calculate_savings(events)
        last_response = {"statusCode": 200, "data": results}
    except Exception as e:
        last_response = {"statusCode": 400, "message": str(e)}

    print(last_response)
