from datetime import datetime

def store_transformer(store_id):
    store_name = "Store " + store_id
    return store_name


def customer_transformer(customer_id, email_address, user_agent, user_id_db, resolution):
    return {
        'customer_id': customer_id if customer_id else '-1',
        'email_address': email_address if email_address and email_address.strip() else 'Not Defined',
        'user_agent': user_agent if user_agent and user_agent.strip() else 'Not Defined',
        'user_id_db': user_id_db if user_id_db and user_id_db.strip() else 'Not Defined',
        'resolution': resolution if resolution and resolution.strip() else 'Not Defined'
    }


def date_transformer(time_stamp): 
    if not time_stamp:
        return None
    
    if isinstance(time_stamp, str):
        time_stamp = datetime.fromisoformat(time_stamp)
    
    day_of_week_num = time_stamp.weekday()
    is_weekend = day_of_week_num >= 5
    day_of_year = time_stamp.timetuple().tm_yday
    week_of_year = time_stamp.isocalendar()[1]
    quarter_number = (time_stamp.month - 1) // 3 + 1
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_abbr = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    return {
        'date_id': time_stamp.strftime('%Y%m%d'),
        'full_date': time_stamp.date(),
        'date_of_week': day_names[day_of_week_num],
        'date_of_week_short': day_names_abbr[day_of_week_num],
        'is_weekday_or_weekend': 'weekend' if is_weekend else 'weekday',
        'day_of_month': time_stamp.day,
        'day_of_year': day_of_year,
        'week_of_year': week_of_year,
        'quarter_number': quarter_number,
        'year_number': time_stamp.year,
        'year_month': time_stamp.strftime('%Y%m')
    }
    
