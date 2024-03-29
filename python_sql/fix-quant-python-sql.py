
#        This script should fix the quants,
#
#        How to use it ?
#        ----------------
#
#        1. Adapt the script or odoo code.
#            1.1 Add the print opcode to debug.
#                1.1.1. search for 'def safe_eval' in all code or in odoo/odoo/tools/safe_eval.py
#                1.1.2. add the print opcode:
#                    just above the 'def safe_eval' there is a list of allowed builtins, all the print
#                    ...
#                    'Exception': Exception,
#                    'print': print, <----------------------------- add this line
#                1.1.3. save and restart odoo-bin to take the change into account
#                or
#            1.2 comment all the print in this script (replace all 'print' by '#print')
#        1. Check the global variable below.
#        2. Copy the code in a server action and run it
#
#
#        How does it works ?
#        --------------------
#
#        Here is a very small summary of what it does:
#        take a backup
#        for each for stockable product
#            for each stock_location with 'internal' usage:
#                # realign the quants regarding the stock_move_line
#                look in stock_move_line what quantity should be in this location
#                find the current quantity in stock_quant
#                insert a new quant with the delta
#                # give the more precise quant quantity.
#                find latest inventory adjustment, take note of the date and current quantities
#                apply the stock_move_line delta from the inventory date.
#                as now know what should be the quant value (and what is the current quant value)
#                apply delta and create a new quant if needed
#                merge the quants
#        take a backup
#
#       What are the risks ?
#       --------------------
#
#       Attention to reserved quantities !!!
#       The script shouldn't be executed a second time !!!
#
#       How to improve execution speed ?
#       ---------------------------------
#
#       drop the following indexes if still exists: (the create index are here for information only)
#       -- CREATE INDEX stock_pack_operation_location_id_fkey_mig_idx ON public.stock_move_line USING btree (location_id)
#       drop index stock_pack_operation_location_id_fkey_mig_idx;
#
#       -- CREATE INDEX stock_pack_operation_location_dest_id_fkey_mig_idx ON public.stock_move_line USING btree (location_dest_id)
#       drop index stock_pack_operation_location_dest_id_fkey_mig_idx
#
#
#       Before running the script :
#       ---------------------------
#
#       You can now start the same cron many time ... but you need some preparation
#       - make a backup before and after
#       - create the following table:
#           CREATE TABLE product_locks AS SELECT id, 'f' AS processed FROM product_product;

INVENTORY_LOCATION_ID = 5
TIMESTAMP = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
CRON_ID = datetime.datetime.now().strftime('%f')
MAX_OFFSET = 10000
COMMIT_EACH_PRODUCT = False

def take_v12_backup(before_after):
    "create a backup of stock_quant, stock_move and stock_move_line"

    if before_after not in ['before','after']:
        raise Exception('before_after should be before of after')
    env.cr.execute("CREATE TABLE stock_quant_%s_%s AS SELECT * FROM stock_quant" % (before_after, TIMESTAMP))
    env.cr.execute("CREATE TABLE stock_move_%s_%s AS SELECT * FROM stock_move" % (before_after, TIMESTAMP))
    env.cr.execute("CREATE TABLE stock_move_line_%s_%s AS SELECT * FROM stock_move_line" % (before_after, TIMESTAMP))

def merge_quant(product_id, location_id):
    env.cr.execute("""
        WITH
        dupes AS (
            SELECT min(qq.id) as to_update_quant_id,
                (array_agg(qq.id ORDER BY qq.id))[2:array_length(array_agg(qq.id), 1)] as to_delete_quant_ids,
                SUM(reserved_quantity) as reserved_quantity,
                SUM(quantity) as quantity,
                min(in_date) as in_date,
                min(l.company_id) as company_id
            FROM stock_quant qq
            JOIN stock_location l ON qq.location_id = l.id
            WHERE product_id = %s
            AND qq.location_id = %s
            GROUP BY product_id, qq.location_id
            HAVING count(qq.id) > 1
        ),
        _up AS (
            UPDATE stock_quant q
                SET quantity = d.quantity,
                    reserved_quantity = d.reserved_quantity,
                    in_date = d.in_date,
                    company_id = d.company_id
            FROM dupes d
            WHERE d.to_update_quant_id = q.id
            AND product_id = %s
            AND location_id = %s
        )
   DELETE FROM stock_quant m WHERE m.id in (SELECT unnest(to_delete_quant_ids) FROM dupes)
    """, (product_id, location_id,product_id, location_id,))

def find_delta_move(location_id, product_id, date):
    """return the change of quantity of the product in this location from the date

        eg: find_delta_move(1,2,'2019-09-26') returning -2.
        this means since '2019-09-26', two products has been removed from this location
    """
    delta_query = """
                SELECT
                    sum(quantity)
                FROM
                (
                    SELECT
                        - COALESCE(SUM(qty_done),0) AS quantity
                    FROM
                        stock_move_line l
                        JOIN stock_move m ON l.move_id=m.id
                    WHERE
                        m.state = 'done'
                        AND l.date > %s
                        AND l.product_id =%s
                        AND l.location_id = %s
                        AND m.inventory_id IS NULL
                UNION ALL
                    SELECT
                        COALESCE(SUM(qty_done),0) AS quantity
                    FROM
                        stock_move_line l
                        JOIN stock_move m ON l.move_id=m.id
                    WHERE
                        m.state = 'done'
                        AND l.date > %s
                        AND l.product_id = %s
                        AND l.location_dest_id = %s
                        AND m.inventory_id IS NULL
                )
                AS ml
    """
    env.cr.execute(delta_query,(date,product_id,location_id,date,product_id,location_id,))
    return env.cr.fetchone()[0]

def find_latest_inventory_adjustment(product_id, location_id):
    "return the date and product_qty for a product_id, location_id or ('1930-09-26',0,) "

    query = """
        SELECT date, product_qty FROM (
        (
        SELECT il.id, date, product_qty FROM stock_inventory i -- needed to have the state
        JOIN stock_inventory_line il ON il.inventory_id = i.id
        WHERE i.state = 'done'
        AND il.location_id = %s
        AND il.product_id = %s
        )
    UNION ALL
        (SELECT -1 AS id, '1930-09-26' AS date, 0 AS product_qty)
    )A
    ORDER BY date DESC, id DESC
    LIMIT 1
    """
    env.cr.execute(query, (location_id,product_id,))
    res = env.cr.fetchone()
    return (res[0], res[1],)

def find_desired_quant_value(product_id, location_id):
    """ return the most accurate quant value for the product

        the most accurate value is value of the last inventory adjustment
        plus the delta of the quants
    """

    latest_inventory_date, latest_inventory_qty = find_latest_inventory_adjustment(product_id, location_id)
    print('  latest_inventory_date: %s' % latest_inventory_date)
    print('  latest_inventory_qty: %s' % latest_inventory_qty)
    delta_moves = find_delta_move(location_id, product_id, latest_inventory_date)
    print('  delta_moves_since_inventory: %s' % delta_moves)
    return latest_inventory_qty + delta_moves

def sql_inventory_adjustment(product_id, qty, location_id, location_dest_id):
    "create stock_move and stock_move_line but don't update the quant"

    if qty == 0:
        return

    if qty < 0:
        qty = -qty
        location_id, location_dest_id = location_dest_id, location_id

    # get default uom for the product.
    env.cr.execute("""
    SELECT t.uom_id FROM product_product p
    JOIN product_template t ON p.product_tmpl_id = t.id
    WHERE p.id = %s
    """, (product_id,))
    product_uom = env.cr.fetchone()[0]

    insert_move_query ="""
    INSERT INTO stock_move
                (
                    "id",
                    "create_uid",
                    "create_date",
                    "write_uid",
                    "write_date",
                    "date",
                    "date_expected",
                    "procure_method",
                    "company_id",
                    "is_done",
                    "location_dest_id",
                    "location_id",
                    "name",
                    "product_id",
                    "product_uom",
                    "product_uom_qty",
                    "state"
                )
                VALUES
                (
                    Nextval('stock_move_id_seq'), --id
                    1, -- create_uid
                    (Now() at time zone 'UTC'), -- create_date
                    1, -- write uid
                    (Now() at time zone 'UTC'), -- write_date
                    (Now() at time zone 'UTC'), -- date
                    (Now() at time zone 'UTC'), -- date_expected
                    'make_to_stock', -- procure method
                    1, -- company_id
                    't', --is_done
                    %s, ---------------------------------------- location_dest_id
                    %s, ---------------------------------------- location_id
                    %s, ---------------------------------------- name
                    %s, ---------------------------------------- product_id
                    %s, ---------------------------------------- product_uom
                    %s,  --------------------------------------- product_uom_qty
                    'done' --state
                )
                returning id;
    """
    env.cr.execute(insert_move_query , (location_dest_id, location_id, 'correction_script product %s' % product_id, product_id, product_uom, qty,))
    move_id = env.cr.fetchone()[0]

    insert_move_line_query = """
                INSERT INTO "stock_move_line"
                    (   "id",
                        "create_uid",
                        "create_date",
                        "write_uid",
                        "write_date",
                        "date",
                        "done_move",
                        "location_dest_id",
                        "location_id",
                        "move_id",
                        "product_id",
                        "product_uom_id",
                        "product_uom_qty",
                        "qty_done",
                        "done_wo",
                        "product_qty",
                        "state"
                    )
                VALUES
                    (
                        Nextval('stock_move_line_id_seq'), --id
                        1, -- create_uid
                        (Now() at time zone 'UTC'), --create_date
                        1, -- write_uid
                        (Now() at time zone 'UTC'), --write_date
                        (Now() at time zone 'UTC'), --date
                        't', --done_move
                        %s, --------------------------------- location_dest_id
                        %s, --------------------------------- location_id
                        %s, --------------------------------- move_id
                        %s, --------------------------------- product_id
                        %s, --------------------------------- product_uom_id
                        '0.000', -- product_uom_qty
                        %s, --------------------------------- qty_done
                        't', --done_wo
                        0, -- product_qty
                        'done' --state
                    )
    """
    env.cr.execute(insert_move_line_query , (location_dest_id, location_id, move_id, product_id, product_uom, qty,))

    insert_quant_query = """
            INSERT INTO "stock_quant"
            (
                "id",
                "create_uid",
                "create_date",
                "write_uid",
                "write_date",
                "in_date",
                "location_id",
                "product_id",
                "quantity",
                "reserved_quantity"
            )
            VALUES
            (
                Nextval('stock_quant_id_seq'), --id
                1, --create_uid
                (Now() at time zone 'UTC'), --create_date
                1, --write_uid
                (Now() at time zone 'UTC'), --write_date
                (Now() at time zone 'UTC'), --in_date
                %s, ------------------------------------- location_id
                %s, ------------------------------------- product_id
                %s, ------------------------------------- quantity,
                0.0 -- reserved_quantity
            )
    """
    env.cr.execute(insert_quant_query , (location_dest_id, product_id, qty,))

    insert_quant_query = """
            INSERT INTO "stock_quant"
            (
                "id",
                "create_uid",
                "create_date",
                "write_uid",
                "write_date",
                "in_date",
                "location_id",
                "product_id",
                "quantity",
                "reserved_quantity"
            )
            VALUES
            (
                Nextval('stock_quant_id_seq'), --id
                1, --create_uid
                (Now() at time zone 'UTC'), --create_date
                1, --write_uid
                (Now() at time zone 'UTC'), --write_date
                (Now() at time zone 'UTC'), --in_date
                %s, ------------------------------------- location_id
                %s, ------------------------------------- product_id
                %s, ------------------------------------- quantity,
                0.0 -- reserved_quantity
            )
    """
    env.cr.execute(insert_quant_query , (location_id, product_id, -qty,))

def find_current_quant_value(product_id, location_id):
    env.cr.execute("""
        SELECT COALESCE(sum(quantity),0)
        FROM stock_quant
        WHERE location_id = %s
        AND product_id = %s
    """, (location_id, product_id))
    current_quant_value = env.cr.fetchone()[0]
    return current_quant_value

def realign_quant_with_moves(product_id, location_id):
    "makes the quants great again"

    # fix quant with and without company_id
    env.cr.execute("""
                UPDATE stock_quant SET company_id = NULL WHERE id IN
                (
                SELECT q.id FROM stock_quant q
                JOIN stock_location l ON q.location_id = l.id
                WHERE COALESCE(q.company_id, -1) <>  COALESCE(l.company_id, -1)
                AND q.company_id = 1
                AND q.location_id = %s
                AND q.product_id = %s
                );
                """, (location_id, product_id,))
    merge_quant(product_id, location_id)

    env.cr.execute("""
                    SELECT
                        sum(quantity)
                    FROM
                    (
                        SELECT
                            - COALESCE(SUM(qty_done),0) AS quantity
                        FROM
                            stock_move_line l
                            JOIN stock_move m ON l.move_id=m.id
                        WHERE
                            m.state = 'done'
                            AND l.product_id = %s
                            AND l.location_id = %s
                    UNION ALL
                        SELECT
                            COALESCE(SUM(qty_done),0) AS quantity
                        FROM
                            stock_move_line l
                            JOIN stock_move m ON l.move_id=m.id
                        WHERE
                            m.state = 'done'
                            AND l.product_id = %s
                            AND l.location_dest_id = %s
                    )
                    AS ml
                    """,(product_id, location_id, product_id, location_id))

    quant_value_according_to_sml = env.cr.fetchone()[0]

    quant_current_value = find_current_quant_value(product_id, location_id)

    quant_delta = quant_value_according_to_sml - quant_current_value
    print("  align quant with moves (%s)" % quant_delta )
    insert_quant_query = """
        INSERT INTO "stock_quant"
        (
            "id",
            "create_uid",
            "create_date",
            "write_uid",
            "write_date",
            "in_date",
            "location_id",
            "product_id",
            "quantity",
            "reserved_quantity"
        )
        VALUES
        (
            Nextval('stock_quant_id_seq'), --id
            1, --create_uid
            (Now() at time zone 'UTC'), --create_date
            1, --write_uid
            (Now() at time zone 'UTC'), --write_date
            (Now() at time zone 'UTC'), --in_date
            %s, ------------------------------------- location_id
            %s, ------------------------------------- product_id
            %s, ------------------------------------- quantity,
            0.0 -- reserved_quantity
        )
        """
    env.cr.execute(insert_quant_query , (location_id, product_id, quant_delta,))

def set_quants(product_id, location_id):
    "realign the quants"

    quant_desired_value = find_desired_quant_value(product_id, location_id)
    print("  quant_desired_value (%s)" % (quant_desired_value,))
    quant_current_value = find_current_quant_value(product_id, location_id)
    print("  quant_current_value (%s)" % (quant_current_value,))
    quant_delta = quant_desired_value - quant_current_value
    if quant_delta == 0:
        print("  adapt the quant (+0) (already at the good value)")
        return
    elif quant_delta > 0:
        location_dest_id = location_id
        location_id = INVENTORY_LOCATION_ID
        print("  adapt the quant (+%s)" % quant_delta)
    else:
        location_dest_id = INVENTORY_LOCATION_ID
        quant_delta = -quant_delta
        print("  adapt the quant (%s)" % quant_delta)

    sql_inventory_adjustment(product_id, quant_delta, location_id, location_dest_id)

def find_locations(product_id):
    "find possible locations for quants based on sml"
    env.cr.execute("""
    SELECT l.lid AS location_id FROM
        (
        SELECT DISTINCT location_id lid FROM stock_move_line WHERE product_id = %s
        UNION
        SELECT DISTINCT location_dest_id lid FROM stock_move_line WHERE product_id = %s
        )l
        JOIN stock_location ll ON l.lid = ll.id
        WHERE ll.usage = 'internal'
    """, (product_id, product_id,))
    return [r['location_id'] for r in env.cr.dictfetchall()]

def is_stockable_product(product_id):
    env.cr.execute("""
                    SELECT type
                    FROM product_template pt
                    JOIN product_product pp ON pt.id = pp.product_tmpl_id
                    WHERE pp.id = %s
                    """, (product_id,))
    if not env.cr.rowcount:
        print("  no template for product %s" % (product_id,))
        return
    else:
        return env.cr.fetchone()[0] == "product"

def max_product_id():
    env.cr.execute("""SELECT max(id) FROM product_product""")
    return env.cr.fetchone()[0]

def get_next_product(last_product_id):
  found_product = False
  offset = 0
  while not found_product:
    print("get_next_product: offset:%s cron_id:%s" % (offset,CRON_ID,))
    env.cr.execute("savepoint acquire_lock")
    try:
      env.cr.execute("select min(id) + %s from product_locks where processed = 'f' and id > %s ", (offset,last_product_id,))
      min_id = env.cr.fetchone()[0]
      offset += 1
      env.cr.execute("select id from product_locks where id = %s for update nowait",(min_id,))
      if env.cr.rowcount:
        return min_id
      if offset > MAX_OFFSET:
        return
    except Exception as e:
        env.cr.execute('rollback to savepoint acquire_lock')

def processed(product_id):
    env.cr.execute("update product_locks set processed = 't' where id = %s",(product_id,))

def do_the_thing():

    product_id = get_next_product(0)
    while product_id:
        if not is_stockable_product(product_id):
            print("%s - product %s is not a stockable product, skip" %
            (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), product_id,))
            processed(product_id)
            if COMMIT_EACH_PRODUCT:
                env.cr.commit()
            product_id = get_next_product(product_id)
            continue
        location_ids = find_locations(product_id)
        if not location_ids:
            print("%s - no location_id for product %s, skip" %
            (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), product_id,))
            processed(product_id)
            if COMMIT_EACH_PRODUCT:
                env.cr.commit()
            product_id = get_next_product(product_id)
            continue
        for location_id in location_ids:
            print("%s - prepare to handle product %s on location %s (cron: %s)" %
            (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), product_id, location_id, CRON_ID))
            realign_quant_with_moves(product_id, location_id)
            set_quants(product_id,location_id)
            merge_quant(product_id, location_id)
            current_quant = find_current_quant_value(product_id, location_id)
            print("  current quant quantity: %s" % current_quant)
            processed(product_id)
            if COMMIT_EACH_PRODUCT:
                env.cr.commit()
            product_id = get_next_product(product_id)

do_the_thing()