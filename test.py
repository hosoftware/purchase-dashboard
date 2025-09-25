from flask import Flask, jsonify, request, send_from_directory
import re
from flask_mysqldb import MySQL
from flask_cors import CORS
from mysql.connector import Error
from datetime import datetime
from datetime import timedelta
import base64
import redis
import pickle
application = Flask(__name__, static_folder="build/assets", template_folder="build")
#CORS(application, resources={r"/*": {"origins": "http://aries-group.in"}})

# application.config['MYSQL_HOST'] = 'localhost'
# application.config['MYSQL_USER'] = 'ariesgroupin_purchaseapproval'
# application.config['MYSQL_PASSWORD'] = '[A&,24p,[[1b'
# application.config['MYSQL_DB'] = 'ariesgroupin_purchase'

application.config['MYSQL_HOST'] = '172.16.134.20'
application.config['MYSQL_USER'] = 'ariesuser2'
application.config['MYSQL_PASSWORD'] = 'pur@2025'
application.config['MYSQL_DB'] = 'purchasedb'

mysql = MySQL(application)

with application.app_context():
    conn = mysql.connection  # Get the connection
    cursor = conn.cursor()
    cursor.execute("SELECT request_no from purchase_request where requesting_id=1")  # Example query
    print(cursor.fetchall())  # Fetch and print results
    print("Database connected successfully")


@application.after_request
def after_request(response):
    #response.headers['Access-Control-Allow-Origin'] = 'http://aries-group.in'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


# Serve React index.html on root
@application.route("/")
def serve_index():
    return send_from_directory(application.template_folder, "index.html")

# Serve static files (JS, CSS, images)
@application.route("/static/<path:path>")
def serve_static(path):
    return send_from_directory(application.static_folder, path)

@application.route('/api/employee_dummy', methods=['GET'])
def get_employee_dummy():
    name='Prabhiraj Nadaraj'

    # cursor.callproc('GenerateYearlyQuery', (userid,))
    # rows = cursor.fetchall()
    # total_requests = rows[0] if rows else 0
    return jsonify({
            "username": name
        })

@application.route('/api/employee', methods=['GET'])
def get_employee():
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    cursor.execute("SELECT role_id,user_id,employee_id,full_name,role_code FROM users WHERE role_id = 3;")
    users_query = cursor.fetchall()
    # cursor.callproc('GenerateYearlyQuery', (userid,))
    # rows = cursor.fetchall()
    # total_requests = rows[0] if rows else 0
    return jsonify({
            "username": name,
            "userrole": user_role_query,
            "role_code": role_code,
            "users": users_query
        })
# userid = 34 
@application.route('/api/role_code', methods=['GET'])
def get_role_code():
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    
    # cursor.callproc('GenerateYearlyQuery', (userid,))
    # rows = cursor.fetchall()
    # total_requests = rows[0] if rows else 0
    return jsonify({
            "username": name,
            "userrole": user_role_query,
            "role_code": role_code
        })
        
@application.route('/api/analytics', methods=['GET'])
def get_analytics():
    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=False)
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    role_code = user_role_query[0][2]
    cache_key = f"purchase_request_data_{employee_id}"
    cached_data = r.get(cache_key)
    if cached_data:
        # If data is found in cache, return the cached data
        # print("Using cached data")
        return pickle.loads(cached_data)
    # return({ "user": role_code })
    if user_role == 2:
        cursor.callproc('GenerateYearlyQuery', (userid,))
        rows = cursor.fetchall()
        cursor.execute("SELECT count(*) FROM purchase_request pr WHERE pr.user_id = %s AND pr.cancel = 0 AND pr.manager_approval_status=0;",(userid,))
        approval_pending_rows = cursor.fetchall()
        cursor.execute("""
                        SELECT 
                        SUM(IF(cancel=0, 1, 0)) AS total_count,
                        SUM(IF(started_by=0 AND cancel=0, 1, 0)) AS pending_to_start,
                        SUM(IF(management_approval_status=0 AND priority_id=1, 1, 0)) AS pending_approval_high_priority,
                        SUM(IF(management_approval_status=0 AND priority_id=2, 1, 0)) AS pending_approval_medium_priority,
                        SUM(IF(management_approval_status=0 AND priority_id=3, 1, 0)) AS pending_approval_normal_priority,
                        SUM(IF((management_approval_status=0 OR management_approval_status IS NULL), 1, 0)) AS pending_approval,
                        SUM(IF((management_approval_status=0 OR management_approval_status IS NULL) AND (priority_id=0 OR priority_id IS NULL), 1, 0)) AS pending_approval_no_priority,
                        SUM(IF(meterial_delivery=0, 1, 0)) AS pending_meterial_delivery,
                        SUM(IF(meterial_delivery=1, 1, 0)) AS meterial_delivered,
                        SUM(IF(lpo_submission=1, 1, 0)) AS lpo_submission
                    FROM purchase_request
                    WHERE user_id = %s
                    AND cancel = 0;""",(userid,))
        all_data_rows=cursor.fetchall()

        total_requests = rows[0] if rows else 0
        approval_pending = approval_pending_rows[0] if approval_pending_rows else 0
        # Convert database rows into JSON format
        cursor.execute("""
            SELECT MONTH(pr.requesting_date) as month, YEAR(pr.requesting_date) as year, COUNT(*) as monthly_count
            FROM purchase_request pr 
            LEFT JOIN 0_emp ON pr.user_id = 0_emp.id 
            WHERE pr.user_id = %s 
            AND pr.cancel = 0
            GROUP BY YEAR(pr.requesting_date), MONTH(pr.requesting_date)
            ORDER BY YEAR(pr.requesting_date) DESC, MONTH(pr.requesting_date) DESC;
        """,(userid,))
        monthly_rows = cursor.fetchall()

        cursor.execute("""SELECT count(*) FROM purchase_request pr LEFT JOIN purchase_actions ON pr.requesting_id = purchase_actions.requesting_id 
                    WHERE pr.user_id = %s AND pr.cancel = 0 and pr.meterial_delivery=1 and purchase_actions.action_id='METERIAL_DELIVERY'  and purchase_actions.status='FD'
                    ORDER BY purchase_actions.rec_id DESC LIMIT 1;""",(userid,))
        material_delivered = cursor.fetchall()

        cursor.execute("""SELECT 
                                MONTH(pr.material_delivery_date) as month, YEAR(pr.material_delivery_date) as year,
                                COUNT(*) AS monthly_material_delivery_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.user_id = %s
                                AND pr.cancel = 0
                                AND pr.meterial_delivery=1
                                AND pa.action_id = 'METERIAL_DELIVERY'
                                AND pa.status = 'FD'
                            GROUP BY 
                                month
                            ORDER BY 
                                month DESC;""",(userid,))
        monthly_material_delivery_row = cursor.fetchall()
        # Convert database rows into JSON format for monthly data
        cursor.execute("""SELECT 
                                COUNT(*) AS monthly_material_delivery_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.user_id = %s
                                AND pr.cancel = 0
                                AND pr.meterial_delivery=1
                                AND pa.action_id = 'METERIAL_DELIVERY'
                                AND pa.status = 'PD';""",(userid,))
        monthly_material_delivery_partial_row  = cursor.fetchall()
        monthly_data = [
            {
                "month": f"{row[1]}-{row[0]:02d}",
                "monthly_count": row[2]
            }
            for row in monthly_rows
        ]
        overall_material_delivery_count = 0
        for row in monthly_material_delivery_row:
            overall_material_delivery_count += row[2]

        overall_material_delivery_partial_count = 0
        overall_material_delivery_partial_count =monthly_material_delivery_partial_row[0][0] if monthly_material_delivery_partial_row else 0

        
        budget_query = """SELECT 
                                fb.year,
                                fb.month,
                                COALESCE(mb.total_budget, 
                                    (SELECT total_budget 
                                    FROM (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                          FROM budget 
                                          GROUP BY YEAR(added_date), MONTH(added_date)) AS mb2
                                    WHERE mb2.year = fb.year 
                                        AND mb2.month <= fb.month
                                    ORDER BY mb2.month DESC 
                                    LIMIT 1)
                                ) AS total_budget
                            FROM 
                                (SELECT DISTINCT 
                                    y.year, 
                                    m.n AS month
                                FROM 
                                    (SELECT DISTINCT YEAR(added_date) AS year FROM budget) y
                                CROSS JOIN 
                                    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
                                            SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
                                            SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
                                            SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12) AS m) AS fb
                            LEFT JOIN 
                                (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                FROM budget
                                GROUP BY YEAR(added_date), MONTH(added_date)) AS mb
                                ON fb.year = mb.year AND fb.month = mb.month
                            WHERE fb.year = 2024  
                            ORDER BY fb.year DESC, fb.month ASC;"""
        cursor.execute(budget_query)
        budget_query_result = cursor.fetchall()
        budget = []
        for row in budget_query_result:
            budget.append(row[2])
        # Return both total and monthly analytics data
        budget_uti_query = """WITH months AS (
        SELECT 1 AS month UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        ),
        years AS (
            SELECT DISTINCT YEAR(requesting_date) AS year 
            FROM purchase_request 
            WHERE user_id = 49 AND YEAR(requesting_date) = 2024 AND cancel = 0
        ),
        year_months AS (
            SELECT y.year, m.month
            FROM years y CROSS JOIN months m
        )
        SELECT 
            ym.year,
            ym.month,
            ROUND(COALESCE(SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' THEN pr.final_amount
                    ELSE pr.final_amount / COALESCE(ex.rate_buy, 1)
                END
            ), 0), 2) AS total_amount_aed
        FROM year_months ym
        LEFT JOIN purchase_request pr 
            ON YEAR(pr.requesting_date) = ym.year 
            AND MONTH(pr.requesting_date) = ym.month
            AND pr.user_id = %s
        LEFT JOIN 0_exchange_rates ex 
            ON pr.final_amount_currency = ex.curr_code 
            AND ex.rate_type = 'AED'
        GROUP BY ym.year, ym.month
        ORDER BY ym.year DESC, ym.month ASC;"""
        cursor.execute(budget_uti_query,(userid,))
        budget_uti_query_result = cursor.fetchall()
        budget_uti = []
        for rows in budget_uti_query_result:
            budget_uti.append(rows[2])
        query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE 
                                user_id = %s AND cancel = 0;"""
        cursor.execute(query, (userid,))
        result = cursor.fetchall()
        category_wise = """SELECT 
                                        rt.name,  -- Assuming 'request_type_name' is the name of the request type column
                                        COUNT(pr.requesting_id) AS request_count
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        pr.user_id = %s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
        cursor.execute(category_wise, (userid,))
        category_wise_result = cursor.fetchall()
        category_wise_pending = """SELECT 
                                        rt.name, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        pr.user_id = %s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
        cursor.execute(category_wise_pending, (userid,))
        category_wise_pending_result = cursor.fetchall()
        approval_pending_ = """SELECT 
                                                    users.full_name AS manager_name, 
                                                    COUNT(purchase_request.requesting_id) AS pending_count
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1 AND  purchase_request.cancel = 0
                                                GROUP BY 
                                                    purchase_request.approval_send_to, users.full_name;"""
        cursor.execute(approval_pending_, (userid,))
        approval_pending_result = cursor.fetchall()
        exceeding_date = """SELECT 
                                        COUNT(*) AS pending_requests_count
                                    FROM 
                                        purchase_request
                                    WHERE 
                                        user_id = %s AND cancel = 0
                                         AND meterial_delivery = 0
                                        AND expected_delivery_date < CURDATE();"""
        cursor.execute(exceeding_date, (userid,))
        exceeding_date_result = cursor.fetchall()
        utilized_budget = """SELECT 
                                ROUND(
                                    SUM(
                                        CASE 
                                            WHEN purchase_request.final_amount_currency = 'AED' 
                                            THEN purchase_request.final_amount 
                                            ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                        END
                                    ), 2
                                ) AS total_final_amount_in_aed
                            FROM 
                                purchase_request
                            LEFT JOIN 
                                (
                                    SELECT curr_code, rate_buy
                                    FROM 0_exchange_rates 
                                    WHERE rate_type = 'AED'
                                    AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                ) AS latest_rates
                                ON purchase_request.final_amount_currency = latest_rates.curr_code
                            WHERE 
                                purchase_request.user_id = %s AND   purchase_request.cancel = 0;"""
        cursor.execute(utilized_budget, (userid,))
        utilized_budget_result = cursor.fetchall()
        
        
        
        return jsonify({
            "totalRequests": result[0],
            "monthly_data": monthly_data,
            "material_delivered": all_data_rows,
            "overall_material_delivery_count": overall_material_delivery_count,
            "overall_material_delivery_partial_count": overall_material_delivery_partial_count,
            "approval_pending_reqs": approval_pending,
            "userName": total_requests[1],
            "budget": budget,
            "budget_uti": budget_uti,
            "userrole": user_role_query,
            "category_wise": category_wise_result,
            "category_wise_pending": category_wise_pending_result,
            "approval_pending": approval_pending_result,
            "exceeding_date": exceeding_date_result,
            "utilized_budget": utilized_budget_result,
            
        })
    elif user_role == 3:
        
        if role_code != 'pmngr1':
            cursor.callproc('PurchaseReviewerYearly', (employee_id,))
            rows = cursor.fetchall()
            total_requests = rows[0] if rows else 0
        else:
            total_requests = (0, 0)
        
        
        query = """SELECT 
                        COUNT(*) AS totalRequests, 
                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                    FROM 
                        purchase_request 
                    WHERE 
                        purchase_in_charge = %s AND cancel = 0;"""
        cursor.execute(query, (employee_id,))
        result = cursor.fetchall()
        
        status_wise = """SELECT 
                                pa.action_id,
                                COUNT(pr.requesting_id) AS request_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s
                                AND pa.is_current = 1
                                AND pr.cancel!=1
                            GROUP BY 
                                pa.action_id
                            ORDER BY 
                                pa.action_id;"""
        cursor.execute(status_wise, (employee_id,))
        status_wise_result = cursor.fetchall()
        status_wise_pending = """SELECT 
                                        pa.action_id, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        purchase_actions pa ON pr.requesting_id = pa.requesting_id
                                    WHERE 
                                        pr.purchase_in_charge = %s
                                        AND pa.is_current = 1
                                        AND pr.cancel = 0
                                    GROUP BY 
                                        pa.action_id
                                    ORDER BY 
                                        pa.action_id;"""
        cursor.execute(status_wise_pending, (employee_id,))
        status_wise_pending_result = cursor.fetchall()
        approval_pending_ = """SELECT 
                                    
                                    users.full_name AS manager_name, 
                                    COUNT(purchase_request.requesting_id) AS pending_count,
                                    users.user_id
                                FROM 
                                    purchase_request
                                LEFT JOIN 
                                    users ON purchase_request.approval_send_to = users.user_id
                                WHERE 
                                    purchase_request.purchase_in_charge = %s
                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                    AND purchase_request.final_negotiation_status = 1
                                    AND purchase_request.cancel=0
                                GROUP BY 
                                    purchase_request.approval_send_to, users.full_name;"""
        cursor.execute(approval_pending_, (employee_id,))
        approval_pending_result = cursor.fetchall()
        exceeding_date = """SELECT 
                                COUNT(*) AS pending_requests_count
                            FROM 
                                purchase_request
                            WHERE 
                                purchase_in_charge = %s AND cancel=0
                                AND meterial_delivery = 0
                                AND expected_delivery_date < CURDATE();"""
        cursor.execute(exceeding_date, (employee_id,))
        exceeding_date_result = cursor.fetchall()
        lpo_release_pending = """SELECT COUNT(pr.requesting_id) AS request_count
                                FROM purchase_request pr
                            LEFT JOIN purchase_actions pa 
                                ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s  AND pr.cancel=0
                                AND pa.is_current = 1        -- Ensuring only the current stage is considered
                                AND (
                                    -- Pending Condition
                                    (
                                        pr.lpo_submission = 0
                                        AND pr.manager_approval_status = 1
                                        AND pr.next_action_code = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                    OR 
                                    -- On Process Condition
                                    (
                                        pr.next_action_code = 'LPO_SUBMISSION'
                                        AND pr.action_status = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                );"""
        cursor.execute(lpo_release_pending, (employee_id,))
        lpo_release_pending_result = cursor.fetchall()
        utilized_budget = """SELECT 
                                ROUND(
                                    SUM(
                                        CASE 
                                            WHEN purchase_request.final_amount_currency = 'AED' 
                                            THEN purchase_request.final_amount 
                                            ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                        END
                                    ), 2
                                ) AS total_final_amount_in_aed
                            FROM 
                                purchase_request
                            LEFT JOIN 
                                (
                                    SELECT curr_code, rate_buy
                                    FROM 0_exchange_rates 
                                    WHERE rate_type = 'AED'
                                    AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                ) AS latest_rates
                                ON purchase_request.final_amount_currency = latest_rates.curr_code
                            WHERE 
                                purchase_request.purchase_in_charge = %s AND purchase_request.cancel=0;"""
        cursor.execute(utilized_budget, (employee_id,))
        utilized_budget_result = cursor.fetchall()
        follow_up = """SELECT 
                            COUNT(CASE WHEN follow_up_date = CURDATE() THEN 1 END) AS today_count,
                            COUNT(CASE WHEN follow_up_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) THEN 1 END) AS yesterday_count,
                            COUNT(CASE WHEN follow_up_date < DATE_SUB(CURDATE(), INTERVAL 1 DAY) THEN 1 END) AS previous_follow_up_count
                        FROM (
                            SELECT DISTINCT purchase_actions.rec_id, follow_up_date
                            FROM purchase_actions
                            LEFT JOIN purchase_request 
                            ON purchase_actions.requesting_id = purchase_request.requesting_id
                            WHERE purchase_actions.is_current = 1 AND purchase_request.cancel=0 AND purchase_actions.action_id!='LPO_SUBMISSION'
                            AND purchase_request.purchase_in_charge = %s
                            AND follow_up_date IS NOT NULL
                        ) AS unique_actions;"""
        cursor.execute(follow_up, (employee_id,))
        follow_up_result = cursor.fetchall()
        
        discount_by_purchase_team = """SELECT 
                                            ROUND(SUM(
                                                CASE 
                                                    WHEN discount_by_purchase_team IS NULL THEN 0
                                                    WHEN discount_by_purchase_team_currency IS NULL OR discount_by_purchase_team_currency = 'AED' THEN discount_by_purchase_team
                                                    ELSE discount_by_purchase_team * COALESCE(
                                                        (SELECT rate_buy FROM 0_exchange_rates 
                                                        WHERE curr_code = purchase_request.discount_by_purchase_team_currency 
                                                        AND rate_type = 'AED' 
                                                        LIMIT 1), 
                                                        1
                                                    )
                                                END
                                            ), 2) AS total_discount,
                                            COUNT(CASE WHEN discount_by_purchase_team IS NOT NULL AND discount_by_purchase_team != 0 THEN 1 END) AS discount_count
                                        FROM purchase_request
                                        WHERE purchase_in_charge = %s AND cancel=0;"""
        cursor.execute(discount_by_purchase_team, (employee_id,))
        discount_by_purchase_team_result = cursor.fetchall()
        
        exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                IFNULL(DATEDIFF(purchase_request.material_delivery_date, purchase_request.expected_delivery_date), 0) AS days_delayed,
                                                IFNULL(DATEDIFF(purchase_request.expected_delivery_date, purchase_request.requesting_date), 0) AS days_expected
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s AND purchase_request.cancel = 0 AND purchase_request.meterial_delivery = 1
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
        cursor.execute(exceeding_date_requests, (employee_id, ))
        exceeding_date_requests_result = cursor.fetchall()
        
        send_to_accounts = """SELECT count(*) as send_to_accounts_count  FROM `purchase_request` WHERE `purchase_in_charge` = %s AND `send_to_accounts` = 1 AND `cancel` = 0;"""
        cursor.execute(send_to_accounts, (employee_id, ))
        send_to_accounts_result = cursor.fetchall()
        
        req_to_md = """SELECT AVG(DATEDIFF(material_delivery_date, requesting_date)) AS avg_days
                                FROM purchase_request
                                WHERE purchase_in_charge = %s AND cancel = 0;"""
        cursor.execute(req_to_md, (employee_id, ))
        req_to_md_result = cursor.fetchall()
        
        req_to_lpo = """SELECT 
                            AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                        FROM purchase_request pr
                        LEFT JOIN purchase_actions pa 
                            ON pr.requesting_id = pa.requesting_id
                            AND pa.action_id = 'LPO_SUBMISSION'
                        WHERE pr.purchase_in_charge = %s
                        AND pr.lpo_submission = 1
                        AND pa.rec_id = (
                            SELECT MAX(pa_sub.rec_id)
                            FROM purchase_actions pa_sub
                            WHERE pa_sub.requesting_id = pr.requesting_id
                            AND pa_sub.action_id = 'LPO_SUBMISSION' AND pr.cancel = 0
                        );"""
        cursor.execute(req_to_lpo, (employee_id, ))
        req_to_lpo_result = cursor.fetchall()
        
        req_to_approval = """SELECT 
                            AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                        FROM purchase_request pr
                        LEFT JOIN purchase_actions pa 
                            ON pr.requesting_id = pa.requesting_id
                            AND pa.action_id = 'MANAGEMENT_APPROVAL'
                        WHERE pr.purchase_in_charge = %s AND pr.cancel = 0
                        AND pr.management_approval_status = 1
                        AND pa.rec_id = (
                            SELECT MAX(pa_sub.rec_id)
                            FROM purchase_actions pa_sub
                            WHERE pa_sub.requesting_id = pr.requesting_id
                            AND pa_sub.action_id = 'MANAGEMENT_APPROVAL'
                        );"""
        cursor.execute(req_to_approval, (employee_id, ))
        
        req_to_approval_result = cursor.fetchall()
        
        icv = """SELECT 
    COUNT(*) AS icv_status_count,
    SUM(
        CASE 
            WHEN final_amount_currency = 'AED' THEN final_amount
            ELSE final_amount * (
                SELECT rate_buy FROM 0_exchange_rates 
                WHERE curr_code = purchase_request.final_amount_currency 
                  AND rate_type = 'AED' 
                LIMIT 1
            )
        END
    ) AS total_final_amount_in_aed
FROM purchase_request
WHERE purchase_in_charge = %s AND cancel = 0
  AND icv_status = 'Y';"""
        cursor.execute(icv, (employee_id, ))
        icv_result = cursor.fetchall()
        budget_query = """SELECT 
                                fb.year,
                                fb.month,
                                COALESCE(mb.total_budget, 
                                    (SELECT total_budget 
                                    FROM (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                          FROM budget 
                                          GROUP BY YEAR(added_date), MONTH(added_date)) AS mb2
                                    WHERE mb2.year = fb.year 
                                        AND mb2.month <= fb.month
                                    ORDER BY mb2.month DESC 
                                    LIMIT 1)
                                ) AS total_budget
                            FROM 
                                (SELECT DISTINCT 
                                    y.year, 
                                    m.n AS month
                                FROM 
                                    (SELECT DISTINCT YEAR(added_date) AS year FROM budget) y
                                CROSS JOIN 
                                    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
                                            SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
                                            SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
                                            SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12) AS m) AS fb
                            LEFT JOIN 
                                (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                FROM budget
                                GROUP BY YEAR(added_date), MONTH(added_date)) AS mb
                                ON fb.year = mb.year AND fb.month = mb.month
                            WHERE fb.year = 2024  
                            ORDER BY fb.year DESC, fb.month ASC;"""
        cursor.execute(budget_query)
        budget_query_result = cursor.fetchall()
        
        target_date_md = """Select count(*) from purchase_request where meterial_delivery=1
                            AND expected_delivery_date >= material_delivery_date
                            AND purchase_in_charge = %s AND cancel = 0"""
        cursor.execute(target_date_md, (employee_id,))
        target_date_md_result = cursor.fetchall()
        invoice_recieved = """SELECT 
                                count(*) as invoice_recieved_count
                            from
                                purchase_request
                            WHERE 
                                purchase_request.invoice_from_supplier = 1
                                AND purchase_request.purchase_in_charge = %s AND cancel = 0;"""
        cursor.execute(invoice_recieved, (employee_id,))
        invoice_recieved_result = cursor.fetchall()

        partial_md = """SELECT count(*)
                            FROM purchase_request
                            LEFT JOIN purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                            WHERE purchase_request.purchase_in_charge = %s AND purchase_request.cancel = 0 AND purchase_actions.status = 'PD'
                            AND purchase_actions.action_id = 'METERIAL_DELIVERY'
                            AND purchase_request.requesting_id NOT IN (
                                SELECT requesting_id FROM purchase_actions WHERE status = 'FD')"""
        cursor.execute(partial_md, (employee_id,))
        partial_md_result = cursor.fetchall()
        
        budget = 0
        for row in budget_query_result:
            budget=budget+row[2]
        date_exceeding_requests_data = [
        {
            "request_no": row[0],
            "description": row[1],
            "person_incharge": row[2],
            "final_amount": row[3],
            "final_amount_currency": row[4],
            "days_delayed": row[5]
        }
        for row in exceeding_date_requests_result
    ]
        total_days_delayed = sum(row[5] for row in exceeding_date_requests_result)
        average_days_delayed = total_days_delayed / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
        total_days_expected = sum(row[6] for row in exceeding_date_requests_result)
        average_days_expected = total_days_expected / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
        total_requests_count = result[0][0] if result else 0
        material_delivery_zero_count = result[0][1] if result else 0
        pending_approvals_count = result[0][2] if result else 0
        exceeding_date_requests_count = exceeding_date_result[0] if result else 0
        lpo_release_pending_count = lpo_release_pending_result[0][0] if result else 0
        pending_approvals_percentage = (int(pending_approvals_count or 0) / int(total_requests_count or 1)) * 100
        material_delivery_zero_percentage = (int(material_delivery_zero_count or 0) / int(total_requests_count or 1)) * 100
        exceeding_date_requests_percentage = (int(exceeding_date_requests_count[0] or 0) / int(total_requests_count or 1)) * 100
        lpo_release_pending_percentage = (int(lpo_release_pending_count or 0) / int(total_requests_count or 1)) * 100



        w1, w2, w3, w4 = 0.4, 0.3, 0.2, 0.1

        kpi = 100 - (
            (w1 * float(pending_approvals_percentage)) +
            (w2 * float(material_delivery_zero_percentage)) +
            (w3 * float(exceeding_date_requests_percentage)) +
            (w4 * float(lpo_release_pending_percentage))
        )
        
        kpi = max(0, min(100, kpi))
        md_pending_requests = """SELECT 
                                            count(*)
                                        FROM 
                                            purchase_request
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_request.meterial_delivery=0 AND purchase_request.cancel = 0"""
                
        cursor.execute(md_pending_requests, (employee_id,))

        md_pending_requests_result = cursor.fetchall()
        
        data = {
            "totalRequests": result[0],
            "userName": total_requests[1],
            "userrole": user_role_query,
            "category_wise": status_wise_result,
            "category_wise_pending": status_wise_pending_result,
            "approval_pending": approval_pending_result,
            "exceeding_date": exceeding_date_result,
            "lpo_pending": lpo_release_pending_result[0],
            "utilized_budget": utilized_budget_result,
            "follow_up": follow_up_result[0],
            "discount_by_purchase_team": discount_by_purchase_team_result[0],
            "kpi": kpi,
            "average_days_delayed": average_days_delayed,
            "average_days_expected": average_days_expected,
            "send_to_accounts": send_to_accounts_result[0][0],
            "req_to_md": req_to_md_result[0][0],
            "req_to_lpo": req_to_lpo_result[0][0],
            "req_to_approval": req_to_approval_result[0][0],
            "icv_status_count": icv_result[0][0],
            "icv_status_amount": icv_result[0][1],
            "budget": budget,
            "target_date_achieved": target_date_md_result,
            "invoice_recieved_count": invoice_recieved_result[0][0],
            "partial_md_count": partial_md_result[0][0],
            "md_pending_requests": md_pending_requests_result
        }
        r.setex(cache_key, 600, pickle.dumps(data))
        return jsonify(data)
    elif role_code == 'mngr':
        current_year = datetime.now().year  
        current_month = datetime.now().month
        option = request.args.get('option') 
        query = """SELECT 
                        COUNT(*) AS totalRequests, 
                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                        AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                    FROM 
                        purchase_request"""
        if option == 'monthly':
            query += """ WHERE
                        YEAR(purchase_request.requesting_date)=%s
                        AND MONTH(purchase_request.requesting_date)=%s AND purchase_request.cancel = 0"""
            cursor.execute(query, (current_year, current_month))
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        
        pendings = """SELECT 
                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                        AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                    FROM 
                        purchase_request
                    WHERE
                        cancel = 0;"""
        cursor.execute(pendings)
        pendings_result = cursor.fetchall()

        expense_head_wise = """SELECT 
                        rt.name,  -- Assuming the column for the name of requesting type in the requesting_type table
                        ROUND(SUM(
                            CASE 
                                WHEN pr.final_amount IS NULL THEN 0
                                WHEN pr.final_amount_currency IS NULL OR pr.final_amount_currency = 'AED' THEN pr.final_amount
                                ELSE pr.final_amount * COALESCE(
                                    (SELECT rate_buy FROM 0_exchange_rates 
                                    WHERE curr_code = pr.final_amount_currency 
                                    AND rate_type = 'AED' 
                                    LIMIT 1), 
                                    1
                                )
                            END
                        ), 2) AS total_final_amount
                    FROM 
                        requesting_type rt
                    LEFT JOIN 
                        purchase_request pr ON pr.rquest_type_id = rt.id"""

        if option == 'monthly':
            expense_head_wise += """ WHERE
                                        YEAR(pr.requesting_date) = %s
                                        AND MONTH(pr.requesting_date) = %s AND pr.cancel=0"""
            expense_head_wise += """ 
                                    GROUP BY 
                                        rt.name
                                    ORDER BY
                                        total_final_amount desc"""  # Ensure GROUP BY is included in the 'monthly' case
            cursor.execute(expense_head_wise, (current_year, current_month))  # Pass current_year and current_month as parameters
        else:
            expense_head_wise += """ 
                                    GROUP BY 
                                        rt.name
                                    ORDER BY
                                        total_final_amount desc"""  # Include GROUP BY in the default case as well
            cursor.execute(expense_head_wise)  # No condition if not 'monthly'

        expense_head_wise_result = cursor.fetchall()
        
        comparison = """SELECT 
    COALESCE(b.year, %s) AS year,  -- Replace NULL year with current_year
    rt.name,  
    ROUND(SUM(
        CASE 
            WHEN pr.final_amount IS NULL THEN 0
            WHEN pr.final_amount_currency IS NULL OR pr.final_amount_currency = 'AED' THEN pr.final_amount
            ELSE pr.final_amount * COALESCE(
                (SELECT rate_buy 
                 FROM 0_exchange_rates 
                 WHERE curr_code = pr.final_amount_currency 
                 AND rate_type = 'AED' 
                 LIMIT 1), 
                1
            )
        END
    ), 2) AS total_final_amount,

    -- Subquery to get total budget once per requesting type, replacing NULL with 0
    COALESCE((
        SELECT ROUND(SUM(b.budget), 2)
        FROM yearly_budget b
        WHERE b.expense_head_id = rt.id
        AND b.year = %s
        AND b.country != %s
        AND b.country != %s
    ), 0) AS total_budget
    
FROM 
    purchase_request pr
LEFT JOIN 
    requesting_type rt ON pr.rquest_type_id = rt.id
LEFT JOIN 
    yearly_budget b ON b.expense_head_id = rt.id 
    AND b.year = %s 
    AND b.country != %s
    AND b.country != %s
WHERE
    pr.company_id IN (SELECT id FROM 0_dimensions WHERE country != %s and country != %s)
    AND YEAR(pr.requesting_date) = %s
GROUP BY 
    rt.name, b.year
ORDER BY 
    b.year, rt.name DESC;"""
        
        cursor.execute(comparison, (current_year, current_year, 8,16, current_year, 8, 16, 8, 16, current_year))  # Pass current_year as parameter
        comparison_result = cursor.fetchall()   
        avg_actuals = sum(row[2] for row in comparison_result)
        comparison_qatar = """SELECT 
    COALESCE(b.year, %s) AS year,  -- Replace NULL year with current_year
    rt.name,  
    ROUND(SUM(
        CASE 
            WHEN pr.final_amount IS NULL THEN 0
            WHEN pr.final_amount_currency IS NULL OR pr.final_amount_currency = 'AED' THEN pr.final_amount
            ELSE pr.final_amount * COALESCE(
                (SELECT rate_buy 
                 FROM 0_exchange_rates 
                 WHERE curr_code = pr.final_amount_currency 
                 AND rate_type = 'AED' 
                 LIMIT 1), 
                1
            )
        END
    ), 2) AS total_final_amount,

    -- Subquery to get total budget once per requesting type, replacing NULL with 0
    COALESCE((
        SELECT ROUND(SUM(b.budget), 2)
        FROM yearly_budget b
        WHERE b.expense_head_id = rt.id
        AND b.year = %s
        AND b.country = %s
    ), 0) AS total_budget
    
FROM 
    purchase_request pr
LEFT JOIN 
    requesting_type rt ON pr.rquest_type_id = rt.id
LEFT JOIN 
    yearly_budget b ON b.expense_head_id = rt.id 
    AND b.year = %s 
    AND b.country = %s
WHERE
    pr.company_id IN (SELECT id FROM 0_dimensions WHERE country = %s)
    AND YEAR(pr.requesting_date) = %s
GROUP BY 
    rt.name, b.year
ORDER BY 
    b.year, rt.name DESC;"""
        
        cursor.execute(comparison_qatar, (current_year, current_year, 16, current_year, 16, 16, current_year))  # Pass current_year as parameter
        comparison_qatar_result = cursor.fetchall()   
        avg_actuals_qatar = sum(row[2] for row in comparison_qatar_result)
        comparison_india = """SELECT 
    COALESCE(b.year, %s) AS year,  -- Replace NULL year with current_year
    rt.name,  
    ROUND(SUM(
        CASE 
            WHEN pr.final_amount IS NULL THEN 0
            WHEN pr.final_amount_currency IS NULL OR pr.final_amount_currency = 'AED' THEN pr.final_amount
            ELSE pr.final_amount * COALESCE(
                (SELECT rate_buy 
                 FROM 0_exchange_rates 
                 WHERE curr_code = pr.final_amount_currency 
                 AND rate_type = 'AED' 
                 LIMIT 1), 
                1
            )
        END
    ), 2) AS total_final_amount,

    -- Subquery to get total budget once per requesting type, replacing NULL with 0
    COALESCE((
        SELECT ROUND(SUM(b.budget), 2)
        FROM yearly_budget b
        WHERE b.expense_head_id = rt.id
        AND b.year = %s
        AND b.country = %s
    ), 0) AS total_budget
    
FROM 
    purchase_request pr
LEFT JOIN 
    requesting_type rt ON pr.rquest_type_id = rt.id
LEFT JOIN 
    yearly_budget b ON b.expense_head_id = rt.id 
    AND b.year = %s 
    AND b.country = %s
WHERE
    pr.company_id IN (SELECT id FROM 0_dimensions WHERE country = %s)
    AND YEAR(pr.requesting_date) = %s
GROUP BY 
    rt.name, b.year
ORDER BY 
    b.year, rt.name DESC;"""
        
        cursor.execute(comparison_india, (current_year, current_year, current_year, 8, current_year, 8, current_year))  # Pass current_year as parameter
        comparison_india_result = cursor.fetchall()
   
        avg_actuals_india = sum(row[2] for row in comparison_india_result)
        budget = """SELECT 
                        SUM(budget)
                    FROM 
                        yearly_budget
                    WHERE
                        year = %s
                        AND country = %s"""
        
        cursor.execute(budget, (current_year, 0))  # Pass current_year as parameter
        budget_result = cursor.fetchall()
        
        budget_qatar = """SELECT 
                        SUM(budget)
                    FROM 
                        yearly_budget
                    WHERE
                        year = %s
                        AND country = %s"""
        
        cursor.execute(budget_qatar, (current_year, 16))  # Pass current_year as parameter
        budget_qatar_result = cursor.fetchall()
        budget_india = """SELECT 
                        SUM(budget)
                    FROM 
                        yearly_budget
                    WHERE
                        year = %s
                        AND country = %s"""
        
        cursor.execute(budget_india, (current_year, 8))  # Pass current_year as parameter
        budget_india_result = cursor.fetchall()
        # You can now return the results
        return jsonify({
            "totalRequests": result[0],
            "role_code": role_code,
            "pendings": pendings_result[0],
            "expense_head_wise": expense_head_wise_result,
            "comparison": comparison_result,
            "comparison_qatar": comparison_qatar_result,
            "comparison_india": comparison_india_result,
            "avgbudget": budget_result[0][0],
            "avgbudgetqatar": budget_qatar_result[0][0],
            "avgbudgetindia": budget_india_result[0][0],
            "avgactuals": avg_actuals,
            "avgactuals_qatar": avg_actuals_qatar,
            "avgactuals_india": avg_actuals_india
        })
# API endpoint to get the request count based on the selected option
@application.route('/api/requests', methods=['GET'])
def get_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option')  # Get the option from the query string
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    current_year = datetime.now().year  # Dynamically get the current year
    current_month = datetime.now().month
    if not option:
        return jsonify({"error": "Option parameter is required"}), 400

    cursor = mysql.connection.cursor()  # Remove dictionary=True
    
    try:
        if user_role == 2:
            if option == "total":
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE 
                                user_id = %s AND cancel = 0;"""
                cursor.execute(query, (userid,))
                result = cursor.fetchall()

                category_wise = """SELECT 
                                        rt.name,  -- Assuming 'request_type_name' is the name of the request type column
                                        COUNT(pr.requesting_id) AS request_count
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        pr.user_id = %s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise, (userid,))
                category_wise_result = cursor.fetchall()

                category_wise_pending = """SELECT 
                                        rt.name, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        pr.user_id = %s
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise_pending, (userid,))
                category_wise_pending_result = cursor.fetchall()
                approval_pending = """SELECT 
                                                    users.full_name AS manager_name, 
                                                    COUNT(purchase_request.requesting_id) AS pending_count
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1
                                                GROUP BY 
                                                    purchase_request.approval_send_to, users.full_name;"""
                cursor.execute(approval_pending, (userid,))
                approval_pending_result = cursor.fetchall()
                exceeding_date = """SELECT 
                                        COUNT(*) AS pending_requests_count
                                    FROM 
                                        purchase_request
                                    WHERE 
                                        user_id = %s 
                                        AND meterial_delivery = 0
                                        AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date, (userid,))
                exceeding_date_result = cursor.fetchall()
                utilized_budget = """SELECT 
                                        ROUND(
                                            SUM(
                                                CASE 
                                                    WHEN purchase_request.final_amount_currency = 'AED' 
                                                    THEN purchase_request.final_amount 
                                                    ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                                END
                                            ), 2
                                        ) AS total_final_amount_in_aed
                                    FROM 
                                        purchase_request
                                    LEFT JOIN 
                                        (
                                            SELECT curr_code, rate_buy
                                            FROM 0_exchange_rates 
                                            WHERE rate_type = 'AED'
                                            AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                        ) AS latest_rates
                                        ON purchase_request.final_amount_currency = latest_rates.curr_code
                                    WHERE 
                                purchase_request.user_id = %s;"""
                cursor.execute(utilized_budget, (userid,))
                utilized_budget_result = cursor.fetchall()
                return jsonify({
                    "totalRequests": result[0], 
                    "category_wise":category_wise_result, 
                    "category_wise_pending":category_wise_pending_result, 
                    "approval_pending": approval_pending_result, 
                    "exceeding_date": exceeding_date_result, 
                    "utilized_budget": utilized_budget_result,
                    "userrole": user_role_query
                    })  # Access result by index for tuple

            elif option == "yearly":
                
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                COALESCE(SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END), 0) AS materialDeliveryZeroCount,
                                COALESCE(SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END), 0) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE YEAR(requesting_date) = %s AND user_id=%s AND cancel = 0
                            """
                cursor.execute(query, (current_year, userid))
                result = cursor.fetchall()
                category_wise_yearly = """SELECT 
                                        rt.name,  -- Assuming 'request_type_name' is the name of the request type column
                                        COUNT(pr.requesting_id) AS request_count
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        YEAR(requesting_date) = %s AND user_id=%s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise_yearly, (current_year, userid))
                category_wise_yearly_result = cursor.fetchall()
                category_wise_pending_yearly = """SELECT 
                                        rt.name, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        YEAR(pr.requesting_date) = %s AND pr.user_id = %s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise_pending_yearly, (current_year, userid))
                category_wise_pending_yearly_result = cursor.fetchall()
                approval_pending_yearly = """SELECT 
                                                    users.full_name AS manager_name, 
                                                    COUNT(purchase_request.requesting_id) AS pending_count
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s AND purchase_request.cancel = 0 AND YEAR(purchase_request.requesting_date) = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1
                                                GROUP BY 
                                                    purchase_request.approval_send_to, users.full_name;"""
                cursor.execute(approval_pending_yearly, (userid,current_year))
                approval_pending_yearly_result = cursor.fetchall()
                exceeding_date_yearly = """SELECT 
                                        COUNT(*) AS pending_requests_count
                                    FROM 
                                        purchase_request
                                    WHERE 
                                        user_id = %s 
                                        AND meterial_delivery = 0
                                        AND YEAR(purchase_request.requesting_date) = %s
                                        AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_yearly, (userid, current_year))
                exceeding_date_yearly_result = cursor.fetchall()
                utilized_budget_yearly = """SELECT 
                                                ROUND(
                                                    SUM(
                                                        CASE 
                                                            WHEN purchase_request.final_amount_currency = 'AED' 
                                                            THEN purchase_request.final_amount 
                                                            ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                                        END
                                                    ), 2
                                                ) AS total_final_amount_in_aed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                (
                                                    SELECT curr_code, rate_buy
                                                    FROM 0_exchange_rates 
                                                    WHERE rate_type = 'AED'
                                                    AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                                ) AS latest_rates
                                                ON purchase_request.final_amount_currency = latest_rates.curr_code
                                            WHERE 
                                purchase_request.user_id = %s
                                AND YEAR(purchase_request.requesting_date) = %s;"""
                cursor.execute(utilized_budget_yearly, (userid, current_year))
                utilized_budget_yearly_result = cursor.fetchall()
                return jsonify({
                    "yearlyRequests": result[0], 
                    "category_wise_yearly": category_wise_yearly_result,
                    "category_wise_pending_yearly":category_wise_pending_yearly_result, 
                    "approval_pending_yearly":approval_pending_yearly_result, 
                    "exceeding_date_yearly": exceeding_date_yearly_result, 
                    "utilized_budget_yearly": utilized_budget_yearly_result,
                    "userrole": user_role_query
                    })  # Access result by index for tuple

            elif option == "monthly":
                  # Dynamically get the current month
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE MONTH(requesting_date) = %s 
                            AND YEAR(requesting_date) = %s
                            AND user_id=%s AND cancel = 0
                            """
                cursor.execute(query, (current_month, current_year, userid))
                result = cursor.fetchall()
                category_wise_monthly = """SELECT 
                                        rt.name,  -- Assuming 'request_type_name' is the name of the request type column
                                        COUNT(pr.requesting_id) AS request_count
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        MONTH(requesting_date) = %s AND
                                        YEAR(requesting_date) = %s AND user_id=%s AND pr.cancel = 0
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise_monthly, (current_month, current_year, userid))
                category_wise_monthly_result = cursor.fetchall()
                category_wise_pending_monthly = """SELECT 
                                        rt.name, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                    FROM 
                                        purchase_request pr
                                    LEFT JOIN 
                                        requesting_type rt ON pr.rquest_type_id = rt.id
                                    WHERE 
                                        MONTH(requesting_date) = %s AND
                                        YEAR(pr.requesting_date) = %s AND pr.user_id = %s
                                    GROUP BY 
                                        rt.name;"""
                cursor.execute(category_wise_pending_monthly, (current_month,current_year, userid))
                category_wise_pending_monthly_result = cursor.fetchall()
                approval_pending_monthly = """SELECT 
                                                    users.full_name AS manager_name, 
                                                    COUNT(purchase_request.requesting_id) AS pending_count
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND YEAR(purchase_request.requesting_date) = %s
                                                    AND MONTH(requesting_date) = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1
                                                GROUP BY 
                                                    purchase_request.approval_send_to, users.full_name;"""
                cursor.execute(approval_pending_monthly, (userid,current_year,current_month))
                approval_pending_monthly_result = cursor.fetchall()
                exceeding_date_monthly = """SELECT 
                                        COUNT(*) AS pending_requests_count
                                    FROM 
                                        purchase_request
                                    WHERE 
                                        user_id = %s 
                                        AND meterial_delivery = 0
                                        AND YEAR(purchase_request.requesting_date) = %s
                                        AND MONTH(purchase_request.requesting_date) = %s
                                        AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_monthly, (userid, current_year, current_month))
                exceeding_date_monthly_result = cursor.fetchall()
                utilized_budget_monthly = """SELECT 
                                                ROUND(
                                                    SUM(
                                                        CASE 
                                                            WHEN purchase_request.final_amount_currency = 'AED' 
                                                            THEN purchase_request.final_amount 
                                                            ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                                        END
                                                    ), 2
                                                ) AS total_final_amount_in_aed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                (
                                                    SELECT curr_code, rate_buy
                                                    FROM 0_exchange_rates 
                                                    WHERE rate_type = 'AED'
                                                    AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                                ) AS latest_rates
                                                ON purchase_request.final_amount_currency = latest_rates.curr_code
                                            WHERE 
                                purchase_request.user_id = %s
                                AND YEAR(purchase_request.requesting_date) = %s
                                AND MONTH(purchase_request.requesting_date) = %s;"""
                cursor.execute(utilized_budget_monthly, (userid, current_year, current_month))
                utilized_budget_monthly_result = cursor.fetchall()
                return jsonify({
                    "monthlyRequests": result[0], 
                    "category_wise_monthly":category_wise_monthly_result, 
                    "category_wise_pending_monthly": category_wise_pending_monthly_result,
                    "approval_pending_monthly": approval_pending_monthly_result, 
                    "exceeding_date_monthly": exceeding_date_monthly_result, 
                    "utilized_budget_monthly": utilized_budget_monthly_result,
                    "userrole": user_role_query
                    })  # Access result by index for tuple

            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            invoice_recieved = """SELECT 
                                count(*) as invoice_recieved_count
                            from
                                purchase_request
                            WHERE 
                                purchase_request.invoice_from_supplier = 1
                                AND purchase_request.purchase_in_charge = %s"""
            cursor.execute(invoice_recieved, (employee_id, ))
            invoice_recieved_result = cursor.fetchall()
            md_pending_yearly_monthly = """SELECT 
                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                        
                    FROM 
                        purchase_request 
                    WHERE 
                        purchase_in_charge = %s AND cancel = 0 """
            if option == 'yearly':
                md_pending_yearly_monthly+= """ AND YEAR(purchase_request.requesting_date) = %s"""
                cursor.execute(md_pending_yearly_monthly, (employee_id, current_year))
            elif option == 'monthly':
                md_pending_yearly_monthly+= """ AND YEAR(purchase_request.requesting_date) = %s 
                                                AND MONTH(purchase_request.requesting_date) = %s"""
                cursor.execute(md_pending_yearly_monthly, (employee_id, current_year, current_month))
            else:
                cursor.execute(md_pending_yearly_monthly, (employee_id,))

            md_pending_yearly_monthly_result = cursor.fetchall()
            invoice_recieved_yearly_monthly = """SELECT 
                                count(*) as invoice_recieved_count
                            from
                                purchase_request
                            WHERE 
                                purchase_request.invoice_from_supplier = 1
                                AND purchase_request.purchase_in_charge = %s"""
            if option == 'yearly':
                invoice_recieved_yearly_monthly += """ AND YEAR(purchase_request.requesting_date) = %s"""
                cursor.execute(invoice_recieved_yearly_monthly, (employee_id, current_year))
            elif option == 'monthly':
                invoice_recieved_yearly_monthly += """ AND YEAR(purchase_request.requesting_date) = %s
                AND MONTH(purchase_request.requesting_date) = %s"""
                cursor.execute(invoice_recieved_yearly_monthly, (employee_id, current_year, current_month))
            else:
                cursor.execute(invoice_recieved_yearly_monthly, (employee_id,))
            invoice_recieved_yearly_monthly_result = cursor.fetchall()
            send_to_accounts_yearly_monthly = """SELECT count(*) as send_to_accounts_count  FROM `purchase_request` WHERE `purchase_in_charge` = %s AND `send_to_accounts` = 1"""
            if option == 'yearly':
                send_to_accounts_yearly_monthly += """ AND YEAR(`purchase_request`.`requesting_date`) = %s"""
                cursor.execute(send_to_accounts_yearly_monthly, (employee_id, current_year))
            elif option == 'monthly':
                send_to_accounts_yearly_monthly += """ AND YEAR(`purchase_request`.`requesting_date`) = %s AND
                                        Month(`purchase_request`.`requesting_date`) = %s"""
                cursor.execute(send_to_accounts_yearly_monthly, (employee_id, current_year, current_month))
            else:
                cursor.execute(send_to_accounts_yearly_monthly, (employee_id, ))
            send_to_accounts_yearly_monthly_result = cursor.fetchall()

            partial_md = """SELECT count(*)
                            FROM purchase_request
                            LEFT JOIN purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                            WHERE purchase_request.purchase_in_charge = %s
                            AND purchase_actions.status = 'PD'
                            AND purchase_actions.action_id = 'METERIAL_DELIVERY'
                            AND purchase_request.requesting_id NOT IN (
                                SELECT requesting_id FROM purchase_actions WHERE status = 'FD')"""
            if option == 'yearly':
                partial_md += """ AND YEAR(purchase_request.requesting_date) = %s"""
                cursor.execute(partial_md, (employee_id, current_year))
            elif option == 'monthly':
                partial_md += """ AND YEAR(purchase_request.requesting_date) = %s 
                AND MONTH(purchase_request.requesting_date) = %s"""
                cursor.execute(partial_md, (employee_id, current_year, current_month))
            else:
                cursor.execute(partial_md, (employee_id,))
            
            partial_md_result = cursor.fetchall()
            if option == "total":
                totalreq="""SELECT
                                        COUNT(*) AS totalRequests, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                                    FROM
                                        purchase_request
                                    WHERE
                                        purchase_request.purchase_in_charge = %s AND cancel = 0;"""
                cursor.execute(totalreq, (employee_id, ))
                total_requests_result = cursor.fetchall()
                rows = cursor.fetchall()
                total_requests = rows[0] if rows else 0
                status_wise = """SELECT 
                                pa.action_id,
                                COUNT(pr.requesting_id) AS request_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s AND pr.cancel = 0
                                AND pa.is_current = 1
                            GROUP BY 
                                pa.action_id
                            ORDER BY 
                                pa.action_id;"""
                cursor.execute(status_wise, (employee_id,))
                status_wise_result = cursor.fetchall()
                status_wise_pending = """SELECT 
                                                pa.action_id, 
                                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount
                                            FROM 
                                                purchase_request pr
                                            LEFT JOIN 
                                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                                            WHERE 
                                                pr.purchase_in_charge = %s
                                                AND pa.is_current = 1
                                            GROUP BY 
                                                pa.action_id
                                            ORDER BY 
                                                pa.action_id;"""
                cursor.execute(status_wise_pending, (employee_id,))
                status_wise_pending_result = cursor.fetchall()
                approval_pending_ = """SELECT 
                                                    users.full_name AS manager_name, 
                                                    COUNT(purchase_request.requesting_id) AS pending_count
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1
                                                GROUP BY 
                                                    purchase_request.approval_send_to, users.full_name;"""
                cursor.execute(approval_pending_, (employee_id,))
                approval_pending_result = cursor.fetchall()
                exceeding_date = """SELECT 
                                                COUNT(*) AS pending_requests_count
                                            FROM 
                                                purchase_request
                                            WHERE 
                                                purchase_in_charge = %s 
                                                AND meterial_delivery = 0
                                                AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date, (employee_id,))
                exceeding_date_result = cursor.fetchall()
                lpo_release_pending = """SELECT COUNT(pr.requesting_id) AS request_count
                                FROM purchase_request pr
                            LEFT JOIN purchase_actions pa 
                                ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s
                                AND pa.is_current = 1        -- Ensuring only the current stage is considered
                                AND (
                                    -- Pending Condition
                                    (
                                        pr.lpo_submission = 0
                                        AND pr.manager_approval_status = 1
                                        AND pr.next_action_code = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                    OR 
                                    -- On Process Condition
                                    (
                                        pr.next_action_code = 'LPO_SUBMISSION'
                                        AND pr.action_status = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                );"""
                cursor.execute(lpo_release_pending, (employee_id,))
                lpo_release_pending_result = cursor.fetchall()
                discount_by_purchase_team = """SELECT 
                                            ROUND(SUM(
                                                CASE 
                                                    WHEN discount_by_purchase_team IS NULL THEN 0
                                                    WHEN discount_by_purchase_team_currency IS NULL OR discount_by_purchase_team_currency = 'AED' THEN discount_by_purchase_team
                                                    ELSE discount_by_purchase_team * COALESCE(
                                                        (SELECT rate_buy FROM 0_exchange_rates 
                                                        WHERE curr_code = purchase_request.discount_by_purchase_team_currency 
                                                        AND rate_type = 'AED' 
                                                        LIMIT 1), 
                                                        1
                                                    )
                                                END
                                            ), 2) AS total_discount,
                                            COUNT(CASE WHEN discount_by_purchase_team IS NOT NULL AND discount_by_purchase_team != 0 THEN 1 END) AS discount_count
                                        FROM purchase_request
                                        WHERE purchase_in_charge = %s;"""
                cursor.execute(discount_by_purchase_team, (employee_id,))
                discount_by_purchase_team_result = cursor.fetchall()
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE 
                                purchase_in_charge = %s AND cancel = 0;"""
                cursor.execute(query, (employee_id,))
                result = cursor.fetchall()
                req_to_md = """SELECT AVG(DATEDIFF(material_delivery_date, requesting_date)) AS avg_days
                                FROM purchase_request
                                WHERE purchase_in_charge = %s;"""
                cursor.execute(req_to_md, (employee_id, ))
                req_to_md_result = cursor.fetchall()
                req_to_lpo = """SELECT 
                            AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                        FROM purchase_request pr
                        LEFT JOIN purchase_actions pa 
                            ON pr.requesting_id = pa.requesting_id
                            AND pa.action_id = 'LPO_SUBMISSION'
                        WHERE pr.purchase_in_charge = %s
                        AND pr.lpo_submission = 1
                        AND pa.rec_id = (
                            SELECT MAX(pa_sub.rec_id)
                            FROM purchase_actions pa_sub
                            WHERE pa_sub.requesting_id = pr.requesting_id
                            AND pa_sub.action_id = 'LPO_SUBMISSION'
                        );"""
                cursor.execute(req_to_lpo, (employee_id,))
                req_to_lpo_result = cursor.fetchall()
                req_to_approval = """SELECT 
                                    AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                                FROM purchase_request pr
                                LEFT JOIN purchase_actions pa 
                                    ON pr.requesting_id = pa.requesting_id
                                    AND pa.action_id = 'MANAGEMENT_APPROVAL'
                                WHERE pr.purchase_in_charge = %s
                                AND pr.management_approval_status = 1
                                AND pa.rec_id = (
                                    SELECT MAX(pa_sub.rec_id)
                                    FROM purchase_actions pa_sub
                                    WHERE pa_sub.requesting_id = pr.requesting_id
                                    AND pa_sub.action_id = 'MANAGEMENT_APPROVAL'
                                );"""
                cursor.execute(req_to_approval, (employee_id, ))
                req_to_approval_result = cursor.fetchall()
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                IFNULL(DATEDIFF(purchase_request.material_delivery_date, purchase_request.expected_delivery_date), 0) AS days_delayed,
                                                IFNULL(DATEDIFF(purchase_request.expected_delivery_date, purchase_request.requesting_date), 0) AS days_expected
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                AND purchase_request.meterial_delivery = 1
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id,))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                total_days_delayed = sum(row[5] for row in exceeding_date_requests_result)
                average_days_delayed = total_days_delayed / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                total_days_expected = sum(row[6] for row in exceeding_date_requests_result)
                average_days_expected = total_days_expected / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                utilized_budget = """SELECT 
                                        ROUND(
                                            SUM(
                                                CASE 
                                                    WHEN purchase_request.final_amount_currency = 'AED' 
                                                    THEN purchase_request.final_amount 
                                                    ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                                END
                                            ), 2
                                        ) AS total_final_amount_in_aed
                                    FROM 
                                        purchase_request
                                    LEFT JOIN 
                                        (
                                            SELECT curr_code, rate_buy
                                            FROM 0_exchange_rates 
                                            WHERE rate_type = 'AED'
                                            AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                        ) AS latest_rates
                                        ON purchase_request.final_amount_currency = latest_rates.curr_code
                                    WHERE 
                                purchase_request.purchase_in_charge = %s;"""
                cursor.execute(utilized_budget, (employee_id, ))
                utilized_budget_result = cursor.fetchall()
                budget_query = """SELECT 
                                fb.year,
                                fb.month,
                                COALESCE(mb.total_budget, 
                                    (SELECT total_budget 
                                    FROM (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                          FROM budget 
                                          GROUP BY YEAR(added_date), MONTH(added_date)) AS mb2
                                    WHERE mb2.year = fb.year 
                                        AND mb2.month <= fb.month
                                    ORDER BY mb2.month DESC 
                                    LIMIT 1)
                                ) AS total_budget
                            FROM 
                                (SELECT DISTINCT 
                                    y.year, 
                                    m.n AS month
                                FROM 
                                    (SELECT DISTINCT YEAR(added_date) AS year FROM budget) y
                                CROSS JOIN 
                                    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
                                            SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
                                            SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
                                            SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12) AS m) AS fb
                            LEFT JOIN 
                                (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                FROM budget
                                GROUP BY YEAR(added_date), MONTH(added_date)) AS mb
                                ON fb.year = mb.year AND fb.month = mb.month
                            WHERE fb.year = 2024  
                            ORDER BY fb.year DESC, fb.month ASC;"""
                cursor.execute(budget_query)
                budget_query_result = cursor.fetchall()
                budget = 0
                for row in budget_query_result:
                    budget=budget+row[2]

                icv = """SELECT 
            COUNT(*) AS icv_status_count,
            SUM(
                CASE 
                    WHEN final_amount_currency = 'AED' THEN final_amount
                    ELSE final_amount * (
                        SELECT rate_buy FROM 0_exchange_rates 
                        WHERE curr_code = purchase_request.final_amount_currency 
                        AND rate_type = 'AED' 
                        LIMIT 1
                    )
                END
            ) AS total_final_amount_in_aed
        FROM purchase_request
        WHERE purchase_in_charge = %s
        AND icv_status = 'Y';"""
                cursor.execute(icv, (employee_id, ))
                icv_result = cursor.fetchall()
                target_date_md = """Select count(*) from purchase_request where meterial_delivery=1
                            AND expected_delivery_date >= material_delivery_date
                            AND purchase_in_charge = %s"""
                cursor.execute(target_date_md, (employee_id, ))
                target_date_md_result = cursor.fetchall()
                
                date_exceeding_requests_data = [
                    {
                        "request_no": row[0],
                        "description": row[1],
                        "person_incharge": row[2],
                        "final_amount": row[3],
                        "final_amount_currency": row[4],
                        "days_delayed": row[5]
                    }
                    for row in exceeding_date_requests_result
                ]
                # total_days_delayed = sum(row[5] for row in exceeding_date_requests_result)
                # average_days_delayed = total_days_delayed / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                total_requests_count = result[0][0] if result else 0
                material_delivery_zero_count = result[0][1] if result else 0
                pending_approvals_count = result[0][2] if result else 0
                exceeding_date_requests_count = exceeding_date_result[0] if result else 0
                lpo_release_pending_count = lpo_release_pending_result[0][0] if result else 0
                pending_approvals_percentage = (pending_approvals_count / total_requests_count) * 100
                material_delivery_zero_percentage = (material_delivery_zero_count / total_requests_count) * 100
                exceeding_date_requests_percentage = (exceeding_date_requests_count[0] / total_requests_count) * 100
                lpo_release_pending_percentage = (lpo_release_pending_count / total_requests_count) * 100



                w1, w2, w3, w4 = 0.4, 0.3, 0.2, 0.1

                kpi = 100 - (
                    (w1 * float(pending_approvals_percentage)) +
                    (w2 * float(material_delivery_zero_percentage)) +
                    (w3 * float(exceeding_date_requests_percentage)) +
                    (w4 * float(lpo_release_pending_percentage))
                )
                
                kpi = max(0, min(100, kpi))
                return jsonify({
                    "totalRequests": total_requests_result,
                    "category_wise": status_wise_result,
                    "category_wise_pending": status_wise_pending_result,
                    "approval_pending": approval_pending_result,
                    "exceeding_date": exceeding_date_result,
                    "lpo_pending": lpo_release_pending_result,
                    "discount_by_purchase_team": discount_by_purchase_team_result,
                    "req_to_md": req_to_md_result[0][0],
                    "req_to_lpo": req_to_lpo_result[0][0],
                    "req_to_approval": req_to_approval_result[0][0],
                    "average_days_delayed": average_days_delayed,
                    "average_days_expected": average_days_expected,
                    "utilized_budget": utilized_budget_result,
                    "budget": budget,
                    "icv_status_count": icv_result[0][0],
                    "icv_status_amount": icv_result[0][1],
                    "kpi": kpi,
                    "userrole": user_role_query,
                    "target_date_achieved": target_date_md_result,
                    "invoice_recieved_count": invoice_recieved_result[0][0],
                    "md_pending_yearly_monthly_result": md_pending_yearly_monthly_result[0][0],
                    "invoice_recieved_yearly_monthly_result": invoice_recieved_yearly_monthly_result[0][0],
                    "send_to_accounts_yearly_monthly_result": send_to_accounts_yearly_monthly_result[0][0],
                    "partial_md_yearly_monthly_count": partial_md_result[0][0],
                    })  # Access result by index for tuple

            elif option == "yearly":
                current_year = datetime.now().year  # Dynamically get the current year
                yearly_requests = """SELECT
                                        COUNT(*) AS totalRequests, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                                    FROM
                                        purchase_request
                                    WHERE
                                        YEAR(purchase_request.requesting_date) = %s
                                        AND purchase_request.purchase_in_charge = %s AND purchase_request.cancel = 0;"""
                cursor.execute(yearly_requests, (current_year, employee_id))
                yearly_requests_result = cursor.fetchall()
                yearly_requests_count = yearly_requests_result[0] if yearly_requests_result else 0
                status_wise = """SELECT 
                                pa.action_id,
                                COUNT(pr.requesting_id) AS request_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s AND pr.cancel = 0
                                AND pa.is_current = 1
                                AND YEAR(pr.requesting_date) = %s
                            GROUP BY 
                                pa.action_id
                            ORDER BY 
                                pa.action_id;"""
                cursor.execute(status_wise, (employee_id, current_year))
                status_wise_result = cursor.fetchall()
                lpo_release_pending = """SELECT COUNT(pr.requesting_id) AS request_count
                                FROM purchase_request pr
                            LEFT JOIN purchase_actions pa 
                                ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s
                                AND YEAR(pr.requesting_date) = %s
                                AND pa.is_current = 1        
                                AND (
                                    
                                    (
                                        pr.lpo_submission = 0
                                        AND pr.manager_approval_status = 1
                                        AND pr.next_action_code = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                    OR 
                                    
                                    (
                                        pr.next_action_code = 'LPO_SUBMISSION'
                                        AND pr.action_status = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                );"""
                cursor.execute(lpo_release_pending, (employee_id, current_year))
                lpo_release_pending_result = cursor.fetchall()
                discount_by_purchase_team = """SELECT 
                                            ROUND(SUM(
                                                CASE 
                                                    WHEN discount_by_purchase_team IS NULL THEN 0
                                                    WHEN discount_by_purchase_team_currency IS NULL OR discount_by_purchase_team_currency = 'AED' THEN discount_by_purchase_team
                                                    ELSE discount_by_purchase_team * COALESCE(
                                                        (SELECT rate_buy FROM 0_exchange_rates 
                                                        WHERE curr_code = purchase_request.discount_by_purchase_team_currency 
                                                        AND rate_type = 'AED' 
                                                        LIMIT 1), 
                                                        1
                                                    )
                                                END
                                            ), 2) AS total_discount,
                                            COUNT(CASE WHEN discount_by_purchase_team IS NOT NULL AND discount_by_purchase_team != 0 THEN 1 END) AS discount_count
                                        FROM purchase_request
                                        WHERE purchase_in_charge = %s
                                        AND YEAR(requesting_date) = %s;"""
                cursor.execute(discount_by_purchase_team, (employee_id, current_year))
                discount_by_purchase_team_result = cursor.fetchall()
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                            FROM 
                                purchase_request 
                            WHERE 
                                purchase_in_charge = %s
                                AND YEAR(requesting_date) = %s AND cancel = 0;"""
                cursor.execute(query, (employee_id, current_year))
                result = cursor.fetchall()
                req_to_md = """SELECT AVG(DATEDIFF(material_delivery_date, requesting_date)) AS avg_days
                                FROM purchase_request
                                WHERE purchase_in_charge = %s
                                AND YEAR(requesting_date) = %s;"""
                cursor.execute(req_to_md, (employee_id, current_year ))
                req_to_md_result = cursor.fetchall()
                req_to_lpo = """SELECT 
                            AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                        FROM purchase_request pr
                        LEFT JOIN purchase_actions pa 
                            ON pr.requesting_id = pa.requesting_id
                            AND pa.action_id = 'LPO_SUBMISSION'
                        WHERE pr.purchase_in_charge = %s
                        AND YEAR(pr.requesting_date) = %s
                        AND pr.lpo_submission = 1
                        AND pa.rec_id = (
                            SELECT MAX(pa_sub.rec_id)
                            FROM purchase_actions pa_sub
                            WHERE pa_sub.requesting_id = pr.requesting_id
                            AND pa_sub.action_id = 'LPO_SUBMISSION'
                        );"""
                cursor.execute(req_to_lpo, (employee_id, current_year))
                req_to_lpo_result = cursor.fetchall()
                req_to_approval = """SELECT 
                                    AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                                FROM purchase_request pr
                                LEFT JOIN purchase_actions pa 
                                    ON pr.requesting_id = pa.requesting_id
                                    AND pa.action_id = 'MANAGEMENT_APPROVAL'
                                WHERE pr.purchase_in_charge = %s
                                AND YEAR(pr.requesting_date) = %s
                                AND pr.management_approval_status = 1
                                AND pa.rec_id = (
                                    SELECT MAX(pa_sub.rec_id)
                                    FROM purchase_actions pa_sub
                                    WHERE pa_sub.requesting_id = pr.requesting_id
                                    AND pa_sub.action_id = 'MANAGEMENT_APPROVAL'
                                );"""
                cursor.execute(req_to_approval, (employee_id, current_year))
                req_to_approval_result = cursor.fetchall()
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                IFNULL(DATEDIFF(purchase_request.material_delivery_date, purchase_request.expected_delivery_date), 0) AS days_delayed,
                                                IFNULL(DATEDIFF(purchase_request.expected_delivery_date, purchase_request.requesting_date), 0) AS days_expected
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 1
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                total_days_delayed = sum(row[5] for row in exceeding_date_requests_result)
                average_days_delayed = total_days_delayed / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                total_days_expected = sum(row[6] for row in exceeding_date_requests_result)
                average_days_expected = total_days_expected / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                utilized_budget = """SELECT 
                                        ROUND(
                                            SUM(
                                                CASE 
                                                    WHEN purchase_request.final_amount_currency = 'AED' 
                                                    THEN purchase_request.final_amount 
                                                    ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                                END
                                            ), 2
                                        ) AS total_final_amount_in_aed
                                    FROM 
                                        purchase_request
                                    LEFT JOIN 
                                        (
                                            SELECT curr_code, rate_buy
                                            FROM 0_exchange_rates 
                                            WHERE rate_type = 'AED'
                                            AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                        ) AS latest_rates
                                        ON purchase_request.final_amount_currency = latest_rates.curr_code
                                    WHERE 
                                purchase_request.purchase_in_charge = %s
                                AND YEAR(purchase_request.requesting_date) = %s;"""
                cursor.execute(utilized_budget, (employee_id, current_year))
                utilized_budget_result = cursor.fetchall()
                budget_query = """SELECT 
                                fb.year,
                                fb.month,
                                COALESCE(mb.total_budget, 
                                    (SELECT total_budget 
                                    FROM (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                          FROM budget 
                                          GROUP BY YEAR(added_date), MONTH(added_date)) AS mb2
                                    WHERE mb2.year = fb.year 
                                        AND mb2.month <= fb.month
                                    ORDER BY mb2.month DESC 
                                    LIMIT 1)
                                ) AS total_budget
                            FROM 
                                (SELECT DISTINCT 
                                    y.year, 
                                    m.n AS month
                                FROM 
                                    (SELECT DISTINCT YEAR(added_date) AS year FROM budget) y
                                CROSS JOIN 
                                    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
                                            SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
                                            SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
                                            SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12) AS m) AS fb
                            LEFT JOIN 
                                (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                FROM budget
                                GROUP BY YEAR(added_date), MONTH(added_date)) AS mb
                                ON fb.year = mb.year AND fb.month = mb.month
                            WHERE fb.year = 2024  
                            ORDER BY fb.year DESC, fb.month ASC;"""
                cursor.execute(budget_query)
                budget_query_result = cursor.fetchall()
                budget = 0
                for row in budget_query_result:
                    budget=budget+row[2]

                icv = """SELECT 
            COUNT(*) AS icv_status_count,
            SUM(
                CASE 
                    WHEN final_amount_currency = 'AED' THEN final_amount
                    ELSE final_amount * (
                        SELECT rate_buy FROM 0_exchange_rates 
                        WHERE curr_code = purchase_request.final_amount_currency 
                        AND rate_type = 'AED' 
                        LIMIT 1
                    )
                END
            ) AS total_final_amount_in_aed
        FROM purchase_request
        WHERE purchase_in_charge = %s
        AND YEAR(requesting_date) = %s
        AND icv_status = 'Y';"""
                cursor.execute(icv, (employee_id, current_year))
                icv_result = cursor.fetchall()
                exceeding_date = """SELECT 
                                                COUNT(*) AS pending_requests_count
                                            FROM 
                                                purchase_request
                                            WHERE 
                                                purchase_in_charge = %s 
                                                AND YEAR(requesting_date) = %s
                                                AND meterial_delivery = 0
                                                AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date, (employee_id, current_year))
                exceeding_date_result = cursor.fetchall()
                target_date_md = """Select count(*) from purchase_request where meterial_delivery=1
                            AND expected_delivery_date >= material_delivery_date
                            AND purchase_in_charge = %s
                            AND YEAR(requesting_date) = %s"""
                cursor.execute(target_date_md, (employee_id, current_year))
                target_date_md_result = cursor.fetchall()
                date_exceeding_requests_data = [
                    {
                        "request_no": row[0],
                        "description": row[1],
                        "person_incharge": row[2],
                        "final_amount": row[3],
                        "final_amount_currency": row[4],
                        "days_delayed": row[5]
                    }
                    for row in exceeding_date_requests_result
                ]
                total_requests_count = result[0][0] if result else 0
                material_delivery_zero_count = result[0][1] if result else 0
                pending_approvals_count = result[0][2] if result else 0
                exceeding_date_requests_count = exceeding_date_result[0] if result else 0
                lpo_release_pending_count = lpo_release_pending_result[0][0] if result else 0
                pending_approvals_percentage = (float(pending_approvals_count or 0) / float(total_requests_count or 1)) * 100
                material_delivery_zero_percentage = (float(material_delivery_zero_count or 0) / float(total_requests_count or 1)) * 100
                exceeding_date_requests_percentage = (float(exceeding_date_requests_count[0] or 0) / float(total_requests_count or 1)) * 100
                lpo_release_pending_percentage = (float(lpo_release_pending_count or 0) / float(total_requests_count or 1)) * 100

                
                
                w1, w2, w3, w4 = 0.4, 0.3, 0.2, 0.1

                kpi = 100 - (
                    (w1 * float(pending_approvals_percentage)) +
                    (w2 * float(material_delivery_zero_percentage)) +
                    (w3 * float(exceeding_date_requests_percentage)) +
                    (w4 * float(lpo_release_pending_percentage))
                )
                if(kpi < 0):
                    kpi = kpi / -1
                
                kpi = max(0, min(100, kpi))
                return jsonify({
                    "yearlyRequests": yearly_requests_result,
                    "category_wise_yearly": status_wise_result,
                    "lpo_pending": lpo_release_pending_result[0],
                    "discount_by_purchase_team": discount_by_purchase_team_result[0],
                    "totalRequests": result[0],
                    "req_to_md": req_to_md_result[0][0],
                    "req_to_lpo": req_to_lpo_result[0][0],
                    "req_to_approval": req_to_approval_result[0][0],
                    "average_days_delayed": average_days_delayed,
                    "average_days_expected": average_days_expected,
                    "utilized_budget": utilized_budget_result,
                    "budget": budget,
                    "icv_status_count": icv_result[0][0],
                    "icv_status_amount": icv_result[0][1],
                    "kpi": kpi,
                    "userrole": user_role_query,
                    "target_date_achieved": target_date_md_result,
                    "invoice_recieved_count": invoice_recieved_result[0][0],
                    "md_pending_yearly_monthly_result": md_pending_yearly_monthly_result[0][0],
                    "invoice_recieved_yearly_monthly_result": invoice_recieved_yearly_monthly_result[0][0],
                    "send_to_accounts_yearly_monthly_result": send_to_accounts_yearly_monthly_result[0][0],
                    "partial_md_yearly_monthly_count": partial_md_result[0][0],
                    })  # Access result by index for tuple

            elif option == "monthly":
                current_year = datetime.now().year  # Dynamically get the current year
                current_month = datetime.now().month  # Dynamically get the current month
                monthly_requests = """SELECT
                                        COUNT(*) AS total_requests_count
                                    FROM
                                        purchase_request
                                    WHERE
                                        YEAR(purchase_request.requesting_date) = %s
                                        AND MONTH(purchase_request.requesting_date) = %s
                                        AND purchase_request.purchase_in_charge = %s
                                        AND purchase_request.cancel!=1;"""
                cursor.execute(monthly_requests, (current_year, current_month, employee_id))
                monthly_requests_result = cursor.fetchall()
                monthly_requests_count = monthly_requests_result[0] if monthly_requests_result else 0
                current_year = datetime.now().year  # Dynamically get the current year
                yearly_requests = """SELECT
                                        COUNT(*) AS totalRequests, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                        AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals
                                    FROM
                                        purchase_request
                                    WHERE
                                        YEAR(purchase_request.requesting_date) = %s
                                        AND MONTH(purchase_request.requesting_date) = %s
                                        AND purchase_request.purchase_in_charge = %s AND purchase_request.cancel = 0"""
                cursor.execute(yearly_requests, (current_year, current_month, employee_id))
                yearly_requests_result = cursor.fetchall()
                all_req_count = """SELECT
                                        COUNT(*) AS totalRequests, 
                                        SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                        SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                        AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals

                                    FROM
                                        purchase_request
                                    WHERE
                                        purchase_request.purchase_in_charge = %s AND purchase_request.cancel != 1;"""
                cursor.execute(all_req_count, (employee_id, ))
                all_req_count_result = cursor.fetchall()
                yearly_requests_count = all_req_count_result[0] if all_req_count_result else 0
                status_wise = """SELECT 
                                pa.action_id,
                                COUNT(pr.requesting_id) AS request_count
                            FROM 
                                purchase_request pr
                            LEFT JOIN 
                                purchase_actions pa ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s AND pr.cancel = 0
                                AND pa.is_current = 1
                                AND YEAR(pr.requesting_date) = %s
                                AND MONTH(pr.requesting_date) = %s
                            GROUP BY 
                                pa.action_id
                            ORDER BY 
                                pa.action_id;"""
                cursor.execute(status_wise, (employee_id, current_year, current_month))
                status_wise_result = cursor.fetchall()
                lpo_release_pending = """SELECT COUNT(pr.requesting_id) AS request_count
                                FROM purchase_request pr
                            LEFT JOIN purchase_actions pa 
                                ON pr.requesting_id = pa.requesting_id
                            WHERE 
                                pr.purchase_in_charge = %s
                                AND YEAR(pr.requesting_date) = %s
                                AND MONTH(pr.requesting_date) = %s
                                AND pa.is_current = 1        
                                AND (
                                    
                                    (
                                        pr.lpo_submission = 0
                                        AND pr.manager_approval_status = 1
                                        AND pr.next_action_code = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                    OR 
                                    
                                    (
                                        pr.next_action_code = 'LPO_SUBMISSION'
                                        AND pr.action_status = 'LPO_SUBMISSION'
                                        AND (
                                            (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                            OR 
                                            (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                        )
                                    )
                                );"""
                cursor.execute(lpo_release_pending, (employee_id, current_year, current_month))
                lpo_release_pending_result = cursor.fetchall()
                discount_by_purchase_team = """SELECT 
                                            ROUND(SUM(
                                                CASE 
                                                    WHEN discount_by_purchase_team IS NULL THEN 0
                                                    WHEN discount_by_purchase_team_currency IS NULL OR discount_by_purchase_team_currency = 'AED' THEN discount_by_purchase_team
                                                    ELSE discount_by_purchase_team * COALESCE(
                                                        (SELECT rate_buy FROM 0_exchange_rates 
                                                        WHERE curr_code = purchase_request.discount_by_purchase_team_currency 
                                                        AND rate_type = 'AED' 
                                                        LIMIT 1), 
                                                        1
                                                    )
                                                END
                                            ), 2) AS total_discount,
                                            COUNT(CASE WHEN discount_by_purchase_team IS NOT NULL AND discount_by_purchase_team != 0 THEN 1 END) AS discount_count
                                        FROM purchase_request
                                        WHERE purchase_in_charge = %s
                                        AND YEAR(requesting_date) = %s
                                        AND MONTH(requesting_date) = %s;"""
                cursor.execute(discount_by_purchase_team, (employee_id, current_year, current_month))
                discount_by_purchase_team_result = cursor.fetchall()
                query = """SELECT 
                                COUNT(*) AS totalRequests, 
                                SUM(CASE WHEN meterial_delivery = 0 THEN 1 ELSE 0 END) AS materialDeliveryZeroCount,
                                SUM(CASE WHEN (management_approval_status = 0 OR management_approval_status IS NULL) 
                                AND final_negotiation_status = 1 THEN 1 ELSE 0 END) AS pendingApprovals

                            FROM 
                                purchase_request 
                            WHERE 
                                purchase_in_charge = %s
                                AND YEAR(requesting_date) = %s
                                AND MONTH(requesting_date) = %s AND cancel = 0"""
                cursor.execute(query, (employee_id, current_year, current_month))
                result = cursor.fetchall()
                req_to_md = """SELECT AVG(DATEDIFF(material_delivery_date, requesting_date)) AS avg_days
                                FROM purchase_request
                                WHERE purchase_in_charge = %s
                                AND YEAR(requesting_date) = %s
                                AND MONTH(requesting_date) = %s;"""
                cursor.execute(req_to_md, (employee_id, current_year, current_month ))
                req_to_md_result = cursor.fetchall()
                req_to_lpo = """SELECT 
                            AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                        FROM purchase_request pr
                        LEFT JOIN purchase_actions pa 
                            ON pr.requesting_id = pa.requesting_id
                            AND pa.action_id = 'LPO_SUBMISSION'
                        WHERE pr.purchase_in_charge = %s
                        AND YEAR(pr.requesting_date) = %s
                        AND MONTH(pr.requesting_date) = %s
                        AND pr.lpo_submission = 1
                        AND pa.rec_id = (
                            SELECT MAX(pa_sub.rec_id)
                            FROM purchase_actions pa_sub
                            WHERE pa_sub.requesting_id = pr.requesting_id
                            AND pa_sub.action_id = 'LPO_SUBMISSION'
                        );"""
                cursor.execute(req_to_lpo, (employee_id, current_year, current_month))
                req_to_lpo_result = cursor.fetchall()
                req_to_approval = """SELECT 
                                    AVG(DATEDIFF(pa.action_date, pr.requesting_date)) AS avg_day_difference
                                FROM purchase_request pr
                                LEFT JOIN purchase_actions pa 
                                    ON pr.requesting_id = pa.requesting_id
                                    AND pa.action_id = 'MANAGEMENT_APPROVAL'
                                WHERE pr.purchase_in_charge = %s
                                AND YEAR(pr.requesting_date) = %s
                                AND MONTH(pr.requesting_date) = %s
                                AND pr.management_approval_status = 1
                                AND pa.rec_id = (
                                    SELECT MAX(pa_sub.rec_id)
                                    FROM purchase_actions pa_sub
                                    WHERE pa_sub.requesting_id = pr.requesting_id
                                    AND pa_sub.action_id = 'MANAGEMENT_APPROVAL'
                                );"""
                cursor.execute(req_to_approval, (employee_id, current_year, current_month))
                req_to_approval_result = cursor.fetchall()
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                IFNULL(DATEDIFF(purchase_request.material_delivery_date, purchase_request.expected_delivery_date), 0) AS days_delayed,
                                                IFNULL(DATEDIFF(purchase_request.expected_delivery_date, purchase_request.requesting_date), 0) AS days_expected
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 1
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                total_days_delayed = sum(row[5] for row in exceeding_date_requests_result)
                total_days_expected = sum(row[6] for row in exceeding_date_requests_result)
                average_days_expected = total_days_expected / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                average_days_delayed = total_days_delayed / len(exceeding_date_requests_result) if exceeding_date_requests_result else 0
                utilized_budget = """SELECT 
                                    ROUND(
                                        SUM(
                                            CASE 
                                                WHEN purchase_request.final_amount_currency = 'AED' 
                                                THEN purchase_request.final_amount 
                                                ELSE purchase_request.final_amount * COALESCE(latest_rates.rate_buy, 1)
                                            END
                                        ), 2
                                    ) AS total_final_amount_in_aed
                                FROM 
                                    purchase_request
                                LEFT JOIN 
                                    (
                                        SELECT curr_code, rate_buy
                                        FROM 0_exchange_rates 
                                        WHERE rate_type = 'AED'
                                        AND id = (SELECT MAX(id) FROM 0_exchange_rates WHERE rate_type = 'AED')
                                    ) AS latest_rates
                                    ON purchase_request.final_amount_currency = latest_rates.curr_code
                                WHERE 
                                purchase_request.purchase_in_charge = %s
                                AND YEAR(purchase_request.requesting_date) = %s
                                AND MONTH(purchase_request.requesting_date) = %s;"""
                cursor.execute(utilized_budget, (employee_id, current_year, current_month))
                utilized_budget_result = cursor.fetchall()
                budget_query = """SELECT 
                                fb.year,
                                fb.month,
                                COALESCE(mb.total_budget, 
                                    (SELECT total_budget 
                                    FROM (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                          FROM budget 
                                          GROUP BY YEAR(added_date), MONTH(added_date)) AS mb2
                                    WHERE mb2.year = fb.year 
                                        AND mb2.month <= fb.month
                                    ORDER BY mb2.month DESC 
                                    LIMIT 1)
                                ) AS total_budget
                            FROM 
                                (SELECT DISTINCT 
                                    y.year, 
                                    m.n AS month
                                FROM 
                                    (SELECT DISTINCT YEAR(added_date) AS year FROM budget) y
                                CROSS JOIN 
                                    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
                                            SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
                                            SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
                                            SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12) AS m) AS fb
                            LEFT JOIN 
                                (SELECT YEAR(added_date) AS year, MONTH(added_date) AS month, SUM(budget) AS total_budget
                                FROM budget
                                GROUP BY YEAR(added_date), MONTH(added_date)) AS mb
                                ON fb.year = mb.year AND fb.month = mb.month
                            WHERE fb.year = 2024  
                            ORDER BY fb.year DESC, fb.month ASC;"""
                cursor.execute(budget_query)
                budget_query_result = cursor.fetchall()
                budget = 0
                for row in budget_query_result:
                    budget=budget+row[2]

                icv = """SELECT 
            COUNT(*) AS icv_status_count,
            SUM(
                CASE 
                    WHEN final_amount_currency = 'AED' THEN final_amount
                    ELSE final_amount * (
                        SELECT rate_buy FROM 0_exchange_rates 
                        WHERE curr_code = purchase_request.final_amount_currency 
                        AND rate_type = 'AED' 
                        LIMIT 1
                    )
                END
            ) AS total_final_amount_in_aed
        FROM purchase_request
        WHERE purchase_in_charge = %s
        AND YEAR(requesting_date) = %s
        AND MONTH(requesting_date) = %s
        AND icv_status = 'Y';"""
                cursor.execute(icv, (employee_id, current_year, current_month))
                icv_result = cursor.fetchall()
                
                exceeding_date = """SELECT 
                                                COUNT(*) AS pending_requests_count
                                            FROM 
                                                purchase_request
                                            WHERE 
                                                purchase_in_charge = %s 
                                                AND YEAR(requesting_date) = %s
                                                AND MONTH(requesting_date) = %s
                                                AND meterial_delivery = 0
                                                AND expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date, (employee_id, current_year, current_month))
                exceeding_date_result = cursor.fetchall()
                
                target_date_md = """Select count(*) from purchase_request where meterial_delivery=1
                            AND expected_delivery_date >= material_delivery_date
                            AND purchase_in_charge = %s
                            AND YEAR(requesting_date) = %s
                            AND MONTH(requesting_date) = %s;"""
                cursor.execute(target_date_md, (employee_id, current_year, current_month))
                target_date_md_result = cursor.fetchall()
                date_exceeding_requests_data = [
                    {
                        "request_no": row[0],
                        "description": row[1],
                        "person_incharge": row[2],
                        "final_amount": row[3],
                        "final_amount_currency": row[4],
                        "days_delayed": row[5]
                    }
                    for row in exceeding_date_requests_result
                ]
                total_requests_count = result[0][0] if result else 0
                material_delivery_zero_count = result[0][1] if result else 0
                pending_approvals_count = result[0][2] if result else 0
                exceeding_date_requests_count = exceeding_date_result[0] if exceeding_date_result else 0
                lpo_release_pending_count = lpo_release_pending_result[0][0] if exceeding_date_result else 0
                pending_approvals_percentage = (float(pending_approvals_count or 0) / float(total_requests_count or 1)) * 100
                material_delivery_zero_percentage = (float(material_delivery_zero_count or 0) / float(total_requests_count or 1)) * 100
                exceeding_date_requests_percentage = (float((exceeding_date_requests_count[0] if exceeding_date_requests_count else 0)) / float(total_requests_count or 1)) * 100
                lpo_release_pending_percentage = (float(lpo_release_pending_count or 0) / float(total_requests_count or 1)) * 100


                
                
                # w1, w2, w3, w4 = 0.4, 0.3, 0.2, 0.1

                # kpi = 100 - (
                #     (w1 * float(pending_approvals_percentage)) +
                #     (w2 * float(material_delivery_zero_percentage)) +
                #     (w3 * float(exceeding_date_requests_percentage)) +
                #     (w4 * float(lpo_release_pending_percentage))
                # )
                # if(kpi < 0):
                #     kpi = kpi / -1
                
                # kpi = max(0, min(100, kpi))
                kpi = 0

                return jsonify({
                    "monthlyRequests": monthly_requests_count,
                    "yearlyRequests": yearly_requests_result,
                    "allReqCount": yearly_requests_count,
                    "category_wise_monthly": status_wise_result,
                    "lpo_pending": lpo_release_pending_result[0],
                    "discount_by_purchase_team": discount_by_purchase_team_result[0],
                    "totalRequests": result[0],
                    "req_to_md": req_to_md_result[0][0],
                    "req_to_lpo": req_to_lpo_result[0][0],
                    "req_to_approval": req_to_approval_result[0][0],
                    "average_days_delayed": average_days_delayed,
                    "average_days_expected": average_days_expected,
                    "utilized_budget": utilized_budget_result,
                    "budget": budget,
                    "icv_status_count": icv_result[0][0],
                    "icv_status_amount": icv_result[0][1],
                    "kpi": kpi,
                    "userrole": user_role_query,
                    "target_date_achieved": target_date_md_result,
                    "invoice_recieved_count": invoice_recieved_result[0][0],
                    "md_pending_yearly_monthly_result": md_pending_yearly_monthly_result[0][0],
                    "invoice_recieved_yearly_monthly_result": invoice_recieved_yearly_monthly_result[0][0],
                    "send_to_accounts_yearly_monthly_result": send_to_accounts_yearly_monthly_result[0][0],
                    "partial_md_yearly_monthly_count": partial_md_result[0][0],
                    })  # Access result by index for tuple
            else:
                return jsonify({"error": "Invalid option"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getDateExceededRequests', methods=['GET'])
def get_exceeding_date_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 2:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s 
                                                
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (employee_id, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getLpoPendingRequests', methods=['GET'])
def get_lpo_pending_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 2:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            if option == "total" or option == "":
                
                lpo_pending_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.management_approval) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            LEFT JOIN
                                                purchase_actions pa ON purchase_request.requesting_id = pa.requesting_id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s  
                                            AND pa.is_current = 1        -- Ensuring only the current stage is considered
                                            AND (
                                                -- Pending Condition
                                                (
                                                    purchase_request.lpo_submission = 0
                                                    AND purchase_request.manager_approval_status = 1
                                                    AND purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                                OR 
                                                -- On Process Condition
                                                (
                                                    purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND purchase_request.action_status = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                            );"""
                cursor.execute(lpo_pending_requests, (employee_id, ))
                lpo_pending_requests_result = cursor.fetchall()
                
                lpo_pending_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in lpo_pending_requests_result
            ]
                return jsonify({ 
                    
                    "lpo_pending_requests": lpo_pending_requests_data
                    })
            elif option == 'yearly':
                lpo_pending_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.management_approval) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            LEFT JOIN
                                                purchase_actions pa ON purchase_request.requesting_id = pa.requesting_id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s  
                                                AND YEAR(purchase_request.requesting_date) = %s
                                            AND pa.is_current = 1        -- Ensuring only the current stage is considered
                                            AND (
                                                -- Pending Condition
                                                (
                                                    purchase_request.lpo_submission = 0
                                                    AND purchase_request.manager_approval_status = 1
                                                    AND purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                                OR 
                                                -- On Process Condition
                                                (
                                                    purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND purchase_request.action_status = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                            );"""
                cursor.execute(lpo_pending_requests, (employee_id, current_year))
                lpo_pending_requests_result = cursor.fetchall()
                lpo_pending_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in lpo_pending_requests_result
            ]
                return jsonify({ 
                    
                    "lpo_pending_requests": lpo_pending_requests_data
                    })
            elif option == 'monthly':
                lpo_pending_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.management_approval) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            LEFT JOIN
                                                purchase_actions pa ON purchase_request.requesting_id = pa.requesting_id
                                            WHERE 
                                                purchase_request.purchase_in_charge = %s  
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                            AND pa.is_current = 1        -- Ensuring only the current stage is considered
                                            AND (
                                                -- Pending Condition
                                                (
                                                    purchase_request.lpo_submission = 0
                                                    AND purchase_request.manager_approval_status = 1
                                                    AND purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                                OR 
                                                -- On Process Condition
                                                (
                                                    purchase_request.next_action_code = 'LPO_SUBMISSION'
                                                    AND purchase_request.action_status = 'LPO_SUBMISSION'
                                                    AND (
                                                        (pa.action_id = 'MANAGER_APPROVAL' AND pa.status = 'C')
                                                        OR 
                                                        (pa.action_id = 'LPO_SUBMISSION' AND pa.status != 'C')
                                                    )
                                                )
                                            );"""
                cursor.execute(lpo_pending_requests, (employee_id, current_year, current_month))
                lpo_pending_requests_result = cursor.fetchall()
                lpo_pending_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in lpo_pending_requests_result
            ]
                return jsonify({ 
                    
                    "lpo_pending_requests": lpo_pending_requests_data
                    })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()


@application.route('/api/getMdPendingRequests', methods=['GET'])
def get_md_pending_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 2:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            md_pending_requests = """SELECT 
                                            purchase_request.request_no,
                                            purchase_request.req_id,
                                            0_emp.name,
                                            purchase_request.final_amount,
                                            purchase_request.final_amount_currency,
                                            purchase_actions.action_id,
                                            purchase_actions.status
                                        FROM 
                                            purchase_request
                                        LEFT JOIN 
                                            0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                        LEFT JOIN 
                                            purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_request.meterial_delivery = 0
                                            AND purchase_actions.is_current=1"""
            cursor.execute(md_pending_requests, (employee_id, ))
            md_pending_requests_result = cursor.fetchall()
            
            md_pending_requests_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "person_incharge": row[2],
                "final_amount": row[3],
                "final_amount_currency": row[4],
                "stage": row[5].replace("_", " ") if row[5] else "",
                "status": "Complete" if row[6] in ("C", "FD", "AP", "SA") else "Pending"
            }
            for row in md_pending_requests_result
        ]
            return jsonify({ 
                
                "md_pending_requests": md_pending_requests_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getAllRequests', methods=['GET'])
def get_all_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 2:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            # if option == 'total' or option=='':
                all_requests = """SELECT 
                                    purchase_request.request_no,
                                    purchase_request.req_id,
                                    0_emp.name,
                                    purchase_request.final_amount,
                                    purchase_request.final_amount_currency,
                                    purchase_actions.action_id,
                                    purchase_actions.status
                                FROM 
                                    purchase_request
                                LEFT JOIN 
                                    0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                LEFT JOIN 
                                    purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                WHERE 
                                    purchase_request.purchase_in_charge = %s
                                    AND purchase_actions.is_current=1"""
                
                if option == 'yearly':
                    all_requests += " AND YEAR(purchase_request.requesting_date) = %s"
                    cursor.execute(all_requests, (employee_id, current_year))  
                elif option == 'monthly':
                    all_requests += " AND YEAR(purchase_request.requesting_date) = %s AND MONTH(purchase_request.requesting_date) = %s"
                    cursor.execute(all_requests, (employee_id, current_year, current_month)) 
                else:
                    cursor.execute(all_requests, (employee_id,))

                all_requests_result = cursor.fetchall()
                
                all_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "stage": row[5].replace("_", " ") if row[5] else "",
                    "status": "Complete" if row[6] in ("C", "FD", "AP", "SA") else "Pending"
                }
                for row in all_requests_result
            ]
                return jsonify({ 
                    
                    "all_requests": all_requests_data
                    })
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getApprovalPendingRequests', methods=['GET'])
def get_approval_pending_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 2:
            if option == "total" or option == "":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, ))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "yearly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            elif option == "monthly":
                exceeding_date_requests = """SELECT 
                                                purchase_request.request_no,
                                                purchase_request.req_id,
                                                0_emp.name,
                                                purchase_request.final_amount,
                                                purchase_request.final_amount_currency,
                                                DATEDIFF(CURDATE(), purchase_request.expected_delivery_date) AS days_delayed
                                            FROM 
                                                purchase_request
                                            LEFT JOIN 
                                                0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                            WHERE 
                                                purchase_request.user_id = %s 
                                                AND YEAR(purchase_request.requesting_date) = %s
                                                AND MONTH(purchase_request.requesting_date) = %s
                                                AND purchase_request.meterial_delivery = 0
                                                AND purchase_request.expected_delivery_date < CURDATE();"""
                cursor.execute(exceeding_date_requests, (userid, current_year, current_month))
                exceeding_date_requests_result = cursor.fetchall()
                date_exceeding_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "person_incharge": row[2],
                    "final_amount": row[3],
                    "final_amount_currency": row[4],
                    "days_delayed": row[5]
                }
                for row in exceeding_date_requests_result
            ]
                return jsonify({ "exceeding_date_requests": date_exceeding_requests_data})
            else:
                return jsonify({"error": "Invalid option"}), 400
        elif user_role == 3:
            # if option == 'total' or option=='':
                approval_pending_requests = """SELECT 
                                                    purchase_request.request_no,
                                                    purchase_request.req_id,
                                                    purchase_request.final_amount,
                                                    purchase_request.final_amount_currency,
                                                    users.full_name AS manager_name
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1;"""
                
                
                cursor.execute(approval_pending_requests, (employee_id,))

                approval_pending_requests_result = cursor.fetchall()
                
                approval_pending_requests_data = [
                {
                    "request_no": row[0],
                    "description": row[1],
                    "final_amount": row[2],
                    "final_amount_currency": row[3],
                    "manager": row[4]
                }
                for row in approval_pending_requests_result
            ]
                return jsonify({ 
                    
                    "approval_pending_requests": approval_pending_requests_data
                    })
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getmdpendingrequests', methods=['GET'])
def get_delivery_pending_requests():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 
    try:
        if user_role == 3:
            # if option == 'total' or option=='':
                md_pending_requests = """SELECT 
                                            count(*)
                                        FROM 
                                            purchase_request
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_request.meterial_delivery=0"""
                if option == 'po':
                   md_pending_requests += """ AND purchase_request.payment_mode = 1"""
                elif option == 'online':
                    md_pending_requests += """ AND purchase_request.payment_mode = 5"""
                elif option == 'supplier':
                    md_pending_requests += """ AND purchase_request.payment_mode = 2"""
                elif option == 'employee':
                    md_pending_requests += """ AND purchase_request.payment_mode = 3"""
                cursor.execute(md_pending_requests, (employee_id,))

                md_pending_requests_result = cursor.fetchall()
                return jsonify({ 
                    "delivery_pending_requests": md_pending_requests_result
                    })
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getmdpendingrequestsyearlymonthly', methods=['GET'])
def get_delivery_pending_requests_yearly_monthly():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    option2 = request.args.get('option2') 
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 
    try:
        if user_role == 3:
            # if option == 'total' or option=='':
                md_pending_requests = """SELECT 
                                            count(*)
                                        FROM 
                                            purchase_request
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_request.meterial_delivery=0"""
                
                if option == 'po':
                   md_pending_requests += """ AND purchase_request.payment_mode = 1"""
                elif option == 'online':
                    md_pending_requests += """ AND purchase_request.payment_mode = 5"""
                elif option == 'supplier':
                    md_pending_requests += """ AND purchase_request.payment_mode = 2"""
                elif option == 'employee':
                    md_pending_requests += """ AND purchase_request.payment_mode = 3"""
                
                if option2 == 'yearly':
                    md_pending_requests += """ AND YEAR(purchase_request.requesting_date) = %s"""
                    cursor.execute(md_pending_requests, (employee_id, current_year))
                elif option2 == 'monthly':
                    md_pending_requests += """ AND YEAR(purchase_request.requesting_date) = %s AND MONTH(purchase_request.requesting_date) = %s"""
                    cursor.execute(md_pending_requests, (employee_id, current_year, current_month))
                else:
                    cursor.execute(md_pending_requests, (employee_id, ))
                md_pending_requests_result = cursor.fetchall()
                return jsonify({ 
                    "delivery_pending_requests_yearly_monthly": md_pending_requests_result
                    })
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getMdPendingRequestsStageWise', methods=['GET'])
def get_md_pending_requests_stage_wise():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    stage = request.args.get('stage')
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 3:
            md_pending_requests_stage_wise = """SELECT 
                                            purchase_request.request_no,
                                            purchase_request.req_id,
                                            0_emp.name,
                                            purchase_request.final_amount,
                                            purchase_request.final_amount_currency,
                                            purchase_actions.action_id,
                                            purchase_actions.status
                                        FROM 
                                            purchase_request
                                        LEFT JOIN 
                                            0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                        LEFT JOIN 
                                            purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_request.meterial_delivery = 0
                                            AND purchase_actions.action_id = %s
                                            AND purchase_actions.is_current=1"""
            cursor.execute(md_pending_requests_stage_wise, (employee_id, stage))
            md_pending_requests_stage_wise_result = cursor.fetchall()
            
            md_pending_requests_stage_wise_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "person_incharge": row[2],
                "final_amount": row[3],
                "final_amount_currency": row[4],
                "stage": row[5].replace("_", " ") if row[5] else "",
                "status": "Complete" if row[6] in ("C", "FD", "AP", "SA") else "Pending"
            }
            for row in md_pending_requests_stage_wise_result
        ]
            return jsonify({ 
                
                "md_pending_requests_stage_wise": md_pending_requests_stage_wise_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getMdPendingRequestsStageWiseYearlyMonthly', methods=['GET'])
def get_md_pending_requests_stage_wise_yearly_monthly():
    cursor = mysql.connection.cursor()
    option = request.args.get('option') 
    stage = request.args.get('stage')
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 3:
            md_pending_requests_stage_wise = """SELECT 
                                            purchase_request.request_no,
                                            purchase_request.req_id,
                                            0_emp.name,
                                            purchase_request.final_amount,
                                            purchase_request.final_amount_currency,
                                            purchase_actions.action_id,
                                            purchase_actions.status
                                        FROM 
                                            purchase_request
                                        LEFT JOIN 
                                            0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                        LEFT JOIN 
                                            purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                        WHERE 
                                            purchase_request.purchase_in_charge = %s
                                            AND purchase_actions.action_id = %s
                                            AND purchase_actions.is_current=1"""
            if option == 'yearly':
                md_pending_requests_stage_wise += " AND YEAR(purchase_request.request_date) = %s "
                cursor.execute(md_pending_requests_stage_wise, (employee_id, stage, current_year))
            elif option == 'monthly':
                md_pending_requests_stage_wise += """ AND YEAR(purchase_request.requesting_date) = %s 
                                                    AND MONTH(purchase_request.requesting_date) = %s"""
                cursor.execute(md_pending_requests_stage_wise, (employee_id, stage, current_year, current_month))
            else:
                cursor.execute(md_pending_requests_stage_wise, (employee_id, stage))
            md_pending_requests_stage_wise_result = cursor.fetchall()
            
            md_pending_requests_stage_wise_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "person_incharge": row[2],
                "final_amount": row[3],
                "final_amount_currency": row[4],
                "stage": row[5].replace("_", " ") if row[5] else "",
                "status": "Complete" if row[6] in ("C", "FD", "AP", "SA") else "Pending"
            }
            for row in md_pending_requests_stage_wise_result
        ]
            return jsonify({ 
                
                "md_pending_requests_stage_wise": md_pending_requests_stage_wise_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()


@application.route('/api/getApPendingRequestsManagerWise', methods=['GET'])
def get_ap_pending_requests_manager_wise():
    cursor = mysql.connection.cursor()
    manager = request.args.get('manager')
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    # if not option:
    #     return jsonify({"error": "Option parameter is required"}), 400

      # Remove dictionary=True

    try:
        if user_role == 3:
            ap_pending_requests_manager_wise = """SELECT 
                                                    purchase_request.request_no,
                                                    purchase_request.req_id,
                                                    purchase_request.final_amount,
                                                    purchase_request.final_amount_currency,
                                                    users.full_name AS manager_name
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    users ON purchase_request.approval_send_to = users.user_id
                                                WHERE 
                                                    purchase_request.purchase_in_charge = %s
                                                    AND purchase_request.approval_send_to = %s
                                                    AND (purchase_request.management_approval_status = 0 or purchase_request.management_approval_status IS NULL)
                                                    AND purchase_request.final_negotiation_status = 1;"""
            
            cursor.execute(ap_pending_requests_manager_wise, (employee_id, manager))
            ap_pending_requests_manager_wise_result = cursor.fetchall()
            
            ap_pending_requests_manager_wise_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "final_amount": row[2],
                "final_amount_currency": row[3],
                "manager": row[4],
            }
            for row in ap_pending_requests_manager_wise_result
        ]
            return jsonify({ 
                
                "ap_pending_requests_manager_wise": ap_pending_requests_manager_wise_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getYesterdayFollowUpRequests', methods=['GET'])
def get_yesterday_follow_up():
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    try:
        if user_role == 3:
            yesterday_follow_up = """SELECT 
                                                    purchase_request.request_no,
                                                    purchase_request.req_id,
                                                    purchase_request.final_amount,
                                                    purchase_request.final_amount_currency,
                                                    0_emp.Name,
                                                    purchase_request.next_action_code
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                                LEFT JOIN 
                                                    0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                                WHERE 
                                                    purchase_actions.is_current = 1
                                                    AND purchase_actions.action_id != 'LPO_SUBMISSION'
                                                    AND purchase_request.purchase_in_charge = %s
                                                    AND purchase_actions.follow_up_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""
            
            cursor.execute(yesterday_follow_up, (employee_id, ))
            yesterday_follow_up_result = cursor.fetchall()
            
            yesterday_follow_up_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "final_amount": row[2],
                "final_amount_currency": row[3],
                "person_incharge": row[4],
                "next_action_code": re.sub(r'_([a-z])', lambda match: match.group(1).upper(), row[5]),
            }
            for row in yesterday_follow_up_result
        ]
            return jsonify({ 
                
                "yesterday_follow_up": yesterday_follow_up_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()

@application.route('/api/getTodayFollowUpRequests', methods=['GET'])
def get_today_follow_up():
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]

    current_year = datetime.now().year
    current_month = datetime.now().month 

    try:
        if user_role == 3:
            today_follow_up = """SELECT 
                                                    purchase_request.request_no,
                                                    purchase_request.req_id,
                                                    purchase_request.final_amount,
                                                    purchase_request.final_amount_currency,
                                                    0_emp.Name,
                                                    purchase_request.next_action_code
                                                FROM 
                                                    purchase_request
                                                LEFT JOIN 
                                                    purchase_actions ON purchase_request.requesting_id = purchase_actions.requesting_id
                                                LEFT JOIN 
                                                    0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                                WHERE 
                                                    purchase_actions.is_current = 1
                                                    AND purchase_actions.action_id != 'LPO_SUBMISSION'
                                                    AND purchase_request.purchase_in_charge = %s
                                                    AND purchase_actions.follow_up_date = CURDATE();"""
            
            cursor.execute(today_follow_up, (employee_id, ))
            today_follow_up_result = cursor.fetchall()
            
            today_follow_up_data = [
            {
                "request_no": row[0],
                "description": row[1],
                "final_amount": row[2],
                "final_amount_currency": row[3],
                "person_incharge": row[4],
                "next_action_code": re.sub(r'_([a-z])', lambda match: match.group(1).upper(), row[5]),
            }
            for row in today_follow_up_result
        ]
            return jsonify({ 
                
                "today_follow_up": today_follow_up_data
                })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        
@application.route('/api/getmdApprovalsPending', methods=['GET'])
def getmdApprovalsPending():
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    mdapprovalspending = """SELECT 
                                    purchase_request.request_no,
                                    purchase_request.req_id,
                                    0_emp.name,
                                    purchase_request.final_amount,
                                    purchase_request.final_amount_currency
                                FROM 
                                    purchase_request
                                LEFT JOIN 
                                    0_emp ON purchase_request.purchase_in_charge = 0_emp.id
                                WHERE 
                                    purchase_request.next_action_user = %s 
                                    AND purchase_request.management_approval_status = 0
                                    AND purchase_request.final_negotiation = 1;"""
    cursor.execute(mdapprovalspending, (userid, ))
    mdapprovalspending_result = cursor.fetchall()
    mdapprovalspending_data = [
    {
        "request_no": row[0],
        "description": row[1],
        "person_incharge": row[2],
        "final_amount": row[3],
        "final_amount_currency": row[4]
    }
    for row in mdapprovalspending_result
]
    return jsonify({ "md_approvals_pending": mdapprovalspending_data})


@application.route('/api/getinvoiceuae', methods=['GET'])
def getInvoiceUae():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    uae_invoice_query = """SELECT 
                        SUM(invoice_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(uae_invoice_query, (current_year, 0))
    uae_invoice_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "uae_invoice": uae_invoice_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getinvoiceqatar', methods=['GET'])
def getInvoiceQatar():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    qatar_invoice_query = """SELECT 
                        SUM(invoice_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(qatar_invoice_query, (current_year, 16))
    qatar_invoice_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "qatar_invoice": qatar_invoice_result[0][0]
    }

    return jsonify(data)


@application.route('/api/getinvoiceindia', methods=['GET'])
def getInvoiceIndia():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    india_invoice_query = """SELECT 
                        SUM(invoice_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(india_invoice_query, (current_year, 8))
    india_invoice_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "india_invoice": india_invoice_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getbudgetuae', methods=['GET'])
def getBudgetUae():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    uae_budget_query = """SELECT 
                        SUM(budget_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(uae_budget_query, (current_year, 0))
    uae_budget_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgbudget_uae": uae_budget_result[0][0]
    }

    return jsonify(data)


@application.route('/api/getbudgetqatar', methods=['GET'])
def getBudgetQatar():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    qatar_budget_query = """SELECT 
                        SUM(budget_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(qatar_budget_query, (current_year, 16))
    qatar_budget_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgbudget_qatar": qatar_budget_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getbudgetindia', methods=['GET'])
def getBudgetIndia():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    india_budget_query = """SELECT 
                        SUM(budget_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(india_budget_query, (current_year, 8))
    india_budget_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgbudget_india": india_budget_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getactualsuae', methods=['GET'])
def getActualsUae():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    uae_atuals_query = """SELECT 
                        SUM(actual_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(uae_atuals_query, (current_year, 0))
    uae_actuals_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgactual_uae": uae_actuals_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getactualsqatar', methods=['GET'])
def getActualsQatar():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    qatar_atuals_query = """SELECT 
                        SUM(actual_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(qatar_atuals_query, (current_year, 16))
    qatar_actuals_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgactual_qatar": qatar_actuals_result[0][0]
    }

    return jsonify(data)

@application.route('/api/getactualsindia', methods=['GET'])
def getActualsIndia():
    current_year = datetime.now().year  
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    india_atuals_query = """SELECT 
                        SUM(actual_amount) / COUNT(DISTINCT month) AS avg_budget
                    FROM 
                        budget_actuals
                    WHERE
                        year = %s
                        AND country_id = %s"""
    cursor.execute(india_atuals_query, (current_year, 8))
    india_actuals_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "avgactual_india": india_actuals_result[0][0]
    }

    return jsonify(data)


@application.route('/api/getcomparison_uae', methods=['GET'])
def getComparisonUae():
    current_year = datetime.now().year  
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    comparison = """SELECT 
                    b.year AS year,
                    rt.name,
                    SUM(b.budget_amount) AS total_budget,
                    SUM(b.actual_amount) AS total_actual_budget
                    FROM 
                    budget_actuals b
                    LEFT JOIN 
                    requesting_type rt ON b.expense_head_id = rt.id
                    WHERE
                    b.year = %s
                    AND b.country_id != %s
                    AND b.country_id != %s
                    GROUP BY 
                    rt.name, b.year
                    ORDER BY 
                    b.year, rt.name DESC;"""

    cursor.execute(comparison, (current_year, 8,16))  # Pass current_year as parameter
    comparison_result = cursor.fetchall()
    data = {
        "role_code": role_code,
        "comparison_uae": comparison_result
    }

    return jsonify(data)

@application.route('/api/getcomparison_qatar', methods=['GET'])
def getComparisonQatar():
    current_year = datetime.now().year  
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    comparison_qatar = """SELECT 
        b.year AS year,
        rt.name,
        SUM(b.budget_amount) AS total_budget,
        SUM(b.actual_amount) AS total_actual_budget
    FROM 
        budget_actuals b
    LEFT JOIN 
        requesting_type rt ON b.expense_head_id = rt.id
    WHERE
        b.year = %s
        AND b.country_id = %s
    GROUP BY 
        rt.name, b.year
    ORDER BY 
        b.year, rt.name DESC;"""
        
    cursor.execute(comparison_qatar, (current_year, 16))  # Pass current_year as parameter
    comparison_qatar_result = cursor.fetchall()   
    data = {
        "role_code": role_code,
        "comparison_qatar": comparison_qatar_result
    }

    return jsonify(data)

@application.route('/api/getcomparison_india', methods=['GET'])
def getComparisonIndia():
    current_year = datetime.now().year  
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    comparison_india = """SELECT 
            b.year AS year,
            rt.name,
            SUM(b.budget_amount) AS total_budget,
            SUM(b.actual_amount) AS total_actual_budget
        FROM 
            budget_actuals b
        LEFT JOIN 
            requesting_type rt ON b.expense_head_id = rt.id
        WHERE
            b.year = %s
            AND b.country_id = %s
        GROUP BY 
            rt.name, b.year
        ORDER BY 
            b.year, rt.name DESC;"""
        
    cursor.execute(comparison_india, (current_year, 8))  # Pass current_year as parameter
    comparison_india_result = cursor.fetchall()   
    data = {
        "role_code": role_code,
        "comparison_india": comparison_india_result
    }

    return jsonify(data)

@application.route('/api/getmngrapprovedcount', methods=['GET'])
def getMngrApprovedCount():
    current_year = datetime.now().year  
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    encoded_user_id = request.args.get('user_id')
    option = request.args.get('option')
    decoded_bytes = base64.b64decode(encoded_user_id)
    userid = decoded_bytes.decode("utf-8")
    cursor.execute("SELECT role_id,employee_id,full_name,role_code FROM users WHERE user_id = %s;",(userid,))
    user_role_query = cursor.fetchall()
    user_role = user_role_query[0][0]
    employee_id = user_role_query[0][1]
    name = user_role_query[0][2]
    role_code = user_role_query[0][3]
    mngrapprovedcount_query = """SELECT 
                        count(*)
                    FROM 
                        purchase_request
                    WHERE
                        approved_by = %s"""
    if option == 'monthly':
        mngrapprovedcount_query += " AND YEAR(management_approval) = %s AND MONTH(management_approval) = %s"
        cursor.execute(mngrapprovedcount_query, (userid, current_year, current_month))
    else:
        cursor.execute(mngrapprovedcount_query, (userid, ))  # Pass current_year and current_month as parameter
    mngrapprovedcount_result = cursor.fetchall()   
    data = {
        "role_code": role_code,
        "mngrapprovedcount": mngrapprovedcount_result[0][0],

    }

    return jsonify(data)


# -------------------------------------------------------------------------MONTHLY REPORT API-------------------------------------------------------------------------------------

@application.route("/api/purchase-request-count", methods=["GET"])
def get_purchase_request_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
;""",(current_year, current_month))
    request_count = cursor.fetchall()
    count = request_count[0][0]
    value = request_count[0][1]
    return jsonify({"count": count, "value": value})

@application.route("/api/carry-forwarded-purchase-request-count", methods=["GET"])
def get_carry_forwarded_purchase_request_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    
    cursor = mysql.connection.cursor()
    cursor.execute("""
        SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
        WHERE 
            (YEAR(requesting_date) < %s OR (YEAR(requesting_date) = %s AND MONTH(requesting_date) < %s))
            AND (
                management_approval IS NULL
                OR (
                    NOT (YEAR(management_approval) = %s AND MONTH(management_approval) = %s)
                )
                   AND (YEAR(requesting_date) >= 2025)
            );
""", (current_year, current_year, current_month, current_year, current_month))

    carry_forwarded_request_count = cursor.fetchall()
    cf_count = carry_forwarded_request_count[0][0]
    cf_value = carry_forwarded_request_count[0][1]
    return jsonify({"cf_count": cf_count, "cf_value": cf_value})

@application.route("/api/purchase-request-approved-count", methods=["GET"])
def get_purchase_request_approved_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.management_approval) = %s 
    AND MONTH(pr.management_approval) = %s
""",(current_year, current_month))
    request_approved_count = cursor.fetchall()
    approved_count = request_approved_count[0][0]
    approved_value = request_approved_count[0][1]
    return jsonify({"approved_count": approved_count, "approved_value": approved_value})

@application.route("/api/purchase-request-lporeleased-count", methods=["GET"])
def get_purchase_request_lporeleased_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.management_approval) = %s 
    AND MONTH(pr.management_approval) = %s
    AND pr.LPO_SUBMISSION = 1""",(current_year, current_month))
    result = cursor.fetchall()
    lporeleased_count = result[0][0]
    lporeleased_value = result[0][1]
    return jsonify({"lporeleased_count": lporeleased_count, "lporeleased_value": lporeleased_value})

@application.route("/api/purchase-request-empadvance-count", methods=["GET"])
def get_purchase_request_empadvance_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
    AND pr.payment_mode = 3;""",(current_year, current_month))
    result = cursor.fetchall()
    empadvance_count = result[0][0]
    empadvance_value = result[0][1]
    return jsonify({"empadvance_count": empadvance_count, "empadvance_value": empadvance_value})

@application.route("/api/purchase-request-onlinepurchase-count", methods=["GET"])
def get_purchase_request_onlinepurchase_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
    AND pr.payment_mode = 5;""",(current_year, current_month))
    result = cursor.fetchall()
    onlinepurchase_count = result[0][0]
    onlinepurchase_value = result[0][1]
    return jsonify({"onlinepurchase_count": onlinepurchase_count, "onlinepurchase_value": onlinepurchase_value})

@application.route("/api/purchase-request-pettycash-count", methods=["GET"])
def get_purchase_request_pettycash_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
    AND pr.payment_mode = 4;""",(current_year, current_month))
    result = cursor.fetchall()
    pettycash_count = result[0][0]
    pettycash_value = result[0][1]
    return jsonify({"pettycash_count": pettycash_count, "pettycash_value": pettycash_value})


@application.route("/api/purchase-request-directlpo-count", methods=["GET"])
def get_purchase_request_directlpo_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
    AND pr.payment_mode = 1;""",(current_year, current_month))
    result = cursor.fetchall()
    directlpo_count = result[0][0]
    directlpo_value = result[0][1]
    return jsonify({"directlpo_count": directlpo_count, "directlpo_value": directlpo_value})

@application.route("/api/purchase-request-materialdelivered-count", methods=["GET"])
def get_purchase_request_materialdelivered_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
    YEAR(pr.requesting_date) = %s 
    AND MONTH(pr.requesting_date) = %s
    AND pr.payment_mode IN (1,3,5)
    AND meterial_delivery = 1;""",(current_year, current_month))
    result = cursor.fetchall()
    materialdelivered_count = result[0][0]
    materialdelivered_value = result[0][1]
    return jsonify({"materialdelivered_count": materialdelivered_count, "materialdelivered_value": materialdelivered_value})

@application.route("/api/purchase-request-materialdeliveredtotal-count", methods=["GET"])
def get_purchase_request_materialdeliveredtotal_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
            ((YEAR(requesting_date) < %s OR (YEAR(requesting_date) = %s AND MONTH(requesting_date) < %s))
            AND (
                management_approval IS NULL
                OR (
                    NOT (YEAR(management_approval) = %s AND MONTH(management_approval) = %s)
                )
                   AND (YEAR(requesting_date) >= 2025)
            )
            OR (YEAR(requesting_date) = %s AND MONTH(requesting_date) = %s))
             AND meterial_delivery = 1;""", (current_year, current_year, current_month, current_year, current_month, current_year, current_month))

    result = cursor.fetchall()
    materialdeliveredtotal_count = result[0][0]
    materialdeliveredtotal_value = result[0][1]
    return jsonify({"materialdeliveredtotal_count": materialdeliveredtotal_count, "materialdeliveredtotal_value": materialdeliveredtotal_value})


@application.route("/api/purchase-request-materialdeliverypendingtotal-count", methods=["GET"])
def get_purchase_request_materialdeliverypendingtotal_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
    COUNT(*) AS total_requests,
    ROUND(
        SUM(
            CASE 
                WHEN pr.final_amount_currency = 'AED' 
                THEN pr.final_amount 
                ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
            END
        ), 2
    ) AS total_final_amount_in_aed
FROM 
    purchase_request pr
LEFT JOIN 
    (
        SELECT curr_code, rate_buy
        FROM 0_exchange_rates 
        WHERE rate_type = 'AED'
        AND id = (
            SELECT MAX(id) 
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
        )
    ) er
    ON pr.final_amount_currency = er.curr_code
WHERE 
            ((YEAR(requesting_date) < %s OR (YEAR(requesting_date) = %s AND MONTH(requesting_date) < %s))
            AND (
                management_approval IS NULL
                OR (
                    NOT (YEAR(management_approval) = %s AND MONTH(management_approval) = %s)
                )
                   AND (YEAR(requesting_date) >= 2025)
            )
            OR (YEAR(requesting_date) = %s AND MONTH(requesting_date) = %s))
                    AND meterial_delivery = 0;""", (current_year, current_year, current_month, current_year, current_month, current_year, current_month))
    result = cursor.fetchall()
    materialdeliverypendingtotal_count = result[0][0]
    materialdeliverypendingtotal_value = result[0][1]
    return jsonify({"materialdeliverypendingtotal_count": materialdeliverypendingtotal_count, "materialdeliverypendingtotal_value": materialdeliverypendingtotal_value})


@application.route("/api/purchase-request-ongoing-count", methods=["GET"])
def get_purchase_request_ongoing_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""
        SELECT 
            COUNT(*) AS total_requests,
            ROUND(
                SUM(
                    CASE 
                        WHEN pr.final_amount_currency = 'AED' 
                        THEN pr.final_amount 
                        ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                    END
                ), 2
            ) AS total_final_amount_in_aed
        FROM 
            purchase_request pr
        LEFT JOIN 
            (
                SELECT curr_code, rate_buy
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
                AND id = (
                    SELECT MAX(id) 
                    FROM 0_exchange_rates 
                    WHERE rate_type = 'AED'
                )
            ) er
            ON pr.final_amount_currency = er.curr_code
        WHERE 
            pr.meterial_delivery != 1
            AND pr.cancel = 0
            AND pr.reject IS NULL
    """)

    result = cursor.fetchall()
    ongoing_count = result[0][0]
    ongoing_value = result[0][1]
    return jsonify({"ongoing_count": ongoing_count, "ongoing_value": ongoing_value})

@application.route("/api/purchase-request-cancel-count", methods=["GET"])
def get_purchase_request_cancel_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
        COUNT(*) AS total_requests,
        ROUND(
            SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' 
                    THEN pr.final_amount 
                    ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                END
            ), 2
        ) AS total_final_amount_in_aed
    FROM 
        purchase_request pr
    LEFT JOIN 
        (
            SELECT curr_code, rate_buy
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
            AND id = (
                SELECT MAX(id) 
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
            )
        ) er
        ON pr.final_amount_currency = er.curr_code
    WHERE 
        YEAR(pr.requesting_date) = %s 
        AND MONTH(pr.requesting_date) = %s
        AND pr.cancel = 1""", (current_year, current_month))

    result = cursor.fetchall()
    cancel_count = result[0][0]  # Total count
    cancel_value = result[0][1] 
    return jsonify({"cancel_count": cancel_count, "cancel_value": cancel_value})

@application.route("/api/purchase-request-billreceived-count", methods=["GET"])
def get_purchase_request_billreceived_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
        COUNT(*) AS total_requests,
        ROUND(
            SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' 
                    THEN pr.final_amount 
                    ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                END
            ), 2
        ) AS total_final_amount_in_aed
    FROM 
        purchase_request pr
    LEFT JOIN 
        (
            SELECT curr_code, rate_buy
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
            AND id = (
                SELECT MAX(id) 
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
            )
        ) er
        ON pr.final_amount_currency = er.curr_code
    WHERE 
        YEAR(pr.invoice_received_date) = %s 
        AND MONTH(pr.invoice_received_date) = %s
        AND pr.invoice_verification = 1
        AND payment_mode IN (1,3,5)""", (current_year, current_month))

    result = cursor.fetchall()
    billreceived_count = result[0][0]  # Total count
    billreceived_value = result[0][1] 
    return jsonify({"billreceived_count": billreceived_count, "billreceived_value": billreceived_value})

@application.route("/api/purchase-request-empadvancebill-count", methods=["GET"])
def get_purchase_request_empadvancebillreceived_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
        COUNT(*) AS total_requests,
        ROUND(
            SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' 
                    THEN pr.final_amount 
                    ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                END
            ), 2
        ) AS total_final_amount_in_aed
    FROM 
        purchase_request pr
    LEFT JOIN 
        (
            SELECT curr_code, rate_buy
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
            AND id = (
                SELECT MAX(id) 
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
            )
        ) er
        ON pr.final_amount_currency = er.curr_code
    WHERE 
        YEAR(pr.requesting_date) = %s 
        AND MONTH(pr.requesting_date) = %s
        AND pr.invoice_verification = 1
        AND payment_mode = 3""", (current_year, current_month))

    result = cursor.fetchall()
    empadvancebill_count = result[0][0]  # Total count
    empadvancebill_value = result[0][1] 
    return jsonify({"empadvancebill_count": empadvancebill_count, "empadvancebill_value": empadvancebill_value})

@application.route("/api/purchase-request-sendtoaccounts-count", methods=["GET"])
def get_purchase_request_sendtoaccounts_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
        COUNT(*) AS total_requests,
        ROUND(
            SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' 
                    THEN pr.final_amount 
                    ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                END
            ), 2
        ) AS total_final_amount_in_aed
    FROM 
        purchase_request pr
    LEFT JOIN 
        (
            SELECT curr_code, rate_buy
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
            AND id = (
                SELECT MAX(id) 
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
            )
        ) er
        ON pr.final_amount_currency = er.curr_code
    WHERE 
        YEAR(pr.requesting_date) = %s 
        AND MONTH(pr.requesting_date) = %s
        AND pr.send_to_accounts = 1 
        AND payment_mode IN (1,3,5)""", (current_year, current_month))

    result = cursor.fetchall()
    sendtoaccounts_count = result[0][0]  # Total count
    sendtoaccounts_value = result[0][1] 
    return jsonify({"sendtoaccounts_count": sendtoaccounts_count, "sendtoaccounts_value": sendtoaccounts_value})

@application.route("/api/purchase-request-billreceivedcurrentmonth-count", methods=["GET"])
def get_purchase_request_billreceivedcurrentmonth_count():
    current_year = datetime.now().year  
    # current_month = datetime.now().month
    current_month = datetime.now().month
    cursor = mysql.connection.cursor()
    cursor.execute("""SELECT 
        COUNT(*) AS total_requests,
        ROUND(
            SUM(
                CASE 
                    WHEN pr.final_amount_currency = 'AED' 
                    THEN pr.final_amount 
                    ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                END
            ), 2
        ) AS total_final_amount_in_aed
    FROM 
        purchase_request pr
    LEFT JOIN 
        (
            SELECT curr_code, rate_buy
            FROM 0_exchange_rates 
            WHERE rate_type = 'AED'
            AND id = (
                SELECT MAX(id) 
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
            )
        ) er
        ON pr.final_amount_currency = er.curr_code
    WHERE 
        YEAR(pr.invoice_received_date) = %s 
        AND MONTH(pr.invoice_received_date) = %s""", (current_year, current_month))

    result = cursor.fetchall()
    billreceivedcurrentmonth_count = result[0][0]  # Total count
    billreceivedcurrentmonth_value = result[0][1] 
    return jsonify({"billreceivedcurrentmonth_count": billreceivedcurrentmonth_count, "billreceivedcurrentmonth_value": billreceivedcurrentmonth_value})



@application.route("/api/purchase-request-billreceivedpreviousmonth-count", methods=["GET"])
def get_purchase_request_billreceivedpreviousmonth_count():
    current_year = datetime.now().year
    current_month = datetime.now().month  # Hardcoded for testing

    # Calculate previous month
    if current_month == 1:
        prev_month = 12
        prev_year = current_year - 1
    else:
        prev_month = current_month - 1
        prev_year = current_year

    cursor = mysql.connection.cursor()
    cursor.execute("""
        SELECT 
            COUNT(*) AS total_requests,
            ROUND(
                SUM(
                    CASE 
                        WHEN pr.final_amount_currency = 'AED' 
                        THEN pr.final_amount 
                        ELSE pr.final_amount * COALESCE(er.rate_buy, 1)
                    END
                ), 2
            ) AS total_final_amount_in_aed
        FROM 
            purchase_request pr
        LEFT JOIN 
            (
                SELECT curr_code, rate_buy
                FROM 0_exchange_rates 
                WHERE rate_type = 'AED'
                AND id = (
                    SELECT MAX(id) 
                    FROM 0_exchange_rates 
                    WHERE rate_type = 'AED'
                )
            ) er
            ON pr.final_amount_currency = er.curr_code
        WHERE 
            YEAR(pr.invoice_received_date) = %s 
            AND MONTH(pr.invoice_received_date) = %s
    """, (prev_year, prev_month))

    result = cursor.fetchall()
    billreceivedpreviousmonth_count = result[0][0]
    billreceivedpreviousmonth_value = result[0][1]
    return jsonify({
        "billreceivedpreviousmonth_count": billreceivedpreviousmonth_count,
        "billreceivedpreviousmonth_value": billreceivedpreviousmonth_value
    })

@application.route("/api/purchase-request-statuswise-count", methods=["GET"])
def get_purchase_request_statuswise_count():
    current_year = datetime.now().year
    current_month = datetime.now().month


    cursor = mysql.connection.cursor()
    cursor.execute("""
        SELECT 
            COUNT(*)
        FROM 
            purchase_request pr
        WHERE 
            YEAR(pr.requesting_date) = %s 
            AND MONTH(pr.requesting_date) = %s
            AND meterial_delivery != 1
            AND cancel=0
            AND reject is null""", (current_year, current_month))
    
    ongoingreqcount = cursor.fetchall()
    ongoingreq_count = ongoingreqcount[0][0]

    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN pr.started_by IS NULL THEN 1 END) AS processnotstarted,
            COUNT(CASE WHEN pr.started_by IS NOT NULL AND pr.quote_review = 0 THEN 1 END) AS processstarted,
            COUNT(CASE WHEN pr.quote_review = 1 AND pr.final_negotiation_status = 0 THEN 1 END) AS reviewofquote,
            COUNT(CASE WHEN pr.final_negotiation_status = 1 AND (pr.management_approval_status = 0 or pr.management_approval_status is null) THEN 1 END) AS finalnegotiation,
            COUNT(CASE WHEN pr.management_approval_status = 1 AND meterial_delivery != 1 THEN 1 END) AS management
        FROM 
            purchase_request pr
        WHERE 
            YEAR(pr.requesting_date) = %s 
            AND MONTH(pr.requesting_date) = %s
            AND meterial_delivery != 1
            AND cancel=0
            AND reject is null""", (current_year, current_month))

    result = cursor.fetchall()
    processnotstarted_count = result[0][0]
    processstarted_count = result[0][1]
    reviewofquote_count = result[0][2]
    finalnegotiation_count = result[0][3]
    managementapproval_count = result[0][4]
    return jsonify({
        "ongoingreq_count": ongoingreq_count,
        "processnotstarted_count": processnotstarted_count,
        "reviewstarted_count": processstarted_count,
        "reviewofquote_count": reviewofquote_count,
        "finalnegotiation_count": finalnegotiation_count,
        "managementapproval_count": managementapproval_count
    })


if __name__ == '__main__':
    application.run(host='172.16.134.36',port=5000,debug=True)
