import json
from collections import defaultdict
from decimal import Decimal
from typing import List, Dict, Any

from sqlalchemy import (
    JSON,
    exists,
    literal_column,
    select,
    case,
    cast,
    String,
    func,
    Numeric,
    and_,
    or_,
    text,
    union_all,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from app.constants.common_enums import OrderSourceEnum
from app.dao.base import BaseDAO
from app.models.v1.sale_order import (
    ComparisonOperator,
    CreateOrderSourceEnum,
    OrderStateEnum,
    SaleOrder,
)
from app.models.v1.sale_order_discount import SaleOrderDiscount
from app.models.v1.sale_order_item import SaleOrderItem
from app.models.v1.sale_order_payment import SaleOrderPayment
from app.models.v1.sale_order_refund import SaleOrderReturn
from app.models.v1.sale_order_refund_item import SaleOrderReturnItem
from app.models.v1.sale_order_refund_payment import SaleOrderRefundPayment
from app.schemas.v1.sale_order import AmountFilter, QueryParamIn, QueryParamPCIn
from app.utils.decimal_helper import format_number_to_display
from app.utils.sql_util import fuzzy_search_string, DEFAULT_ESCAPE_CHAR


class SaleOrderDao(BaseDAO):

    @staticmethod
    def get_state_name(state_value: int) -> str:
        try:
            return OrderStateEnum(state_value).description
        except ValueError:
            return "未知状态"

    @staticmethod
    def convert_aggregated_refund_result_code(
        refund_id: int, aggregated_status_by_refund_id_map: dict
    ):
        """
        统计退款单聚合支付的状态
        :param refund_id:
        :param aggregated_status_by_refund_id_map:
        :return: 0:没有聚合支付 1:退款中 2:退款成功 3:退款失败
        """
        # 没有聚合支付
        if refund_id not in aggregated_status_by_refund_id_map:
            return 0
        is_aggregated_refund_success = aggregated_status_by_refund_id_map.get(refund_id)
        # 退款中
        if is_aggregated_refund_success is None:
            return 1
        # 成功
        if is_aggregated_refund_success:
            return 2
        return 3

    def sale_order_state_trans(self, exclude_state: list[int] = []):
        """状态转换(主要针对销售订单的状态转换)"""
        state_case = case(
            *[
                (SaleOrder.state == state.value, state.description)
                for state in OrderStateEnum
                if state.value not in exclude_state
            ],
            else_="未知状态",
        )
        return state_case

    def order_source_trans(self):
        """订单来源转换"""
        order_source_case = case(
            *[
                (SaleOrder.order_source == source.value, source.description)
                for source in CreateOrderSourceEnum
            ],
            else_="未知来源",
        )
        return order_source_case

    async def do_get_local_order_list_info(self, query_param_in: QueryParamIn):
        """
        分页获取本地订单列表
        """
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        # 转换分页
        offset_count = query_param_in.page_size * (query_param_in.page_number - 1)

        # 构建基础查询条件
        conditions = [
            SaleOrder.disabled.is_(False),
            SaleOrder.company_id == query_param_in.company_id,
            # 门店收银
            SaleOrder.order_source == OrderSourceEnum.STORE_ORDER.code
        ]
        # 添加订单号/用户/商品名称/商品条码搜索条件
        if query_param_in.order_num_or_user:
            keywords_search_ilike_str = fuzzy_search_string(
                string=query_param_in.order_num_or_user,
                escape_char=DEFAULT_ESCAPE_CHAR,
            )
            conditions.append(
                or_(
                    SaleOrder.order_number.ilike(
                        keywords_search_ilike_str,
                        escape=DEFAULT_ESCAPE_CHAR,
                    ),
                    SaleOrder.member_name.ilike(
                        keywords_search_ilike_str,
                        escape=DEFAULT_ESCAPE_CHAR,
                    ),
                    SaleOrder.member_phone.ilike(
                        keywords_search_ilike_str,
                        escape=DEFAULT_ESCAPE_CHAR,
                    ),
                    # SaleOrder.record_id 与 SaleOrderItem.order_id 关联, 搜索名字和条码
                    exists(
                        select(1)
                        .select_from(SaleOrderItem)
                        .where(
                            and_(
                                SaleOrderItem.order_id == SaleOrder.record_id,
                                SaleOrderItem.disabled.is_(False),
                                # 这里转义
                                or_(
                                    SaleOrderItem.goods_sale_name.ilike(
                                        keywords_search_ilike_str,
                                        escape=DEFAULT_ESCAPE_CHAR,
                                    ),
                                    # 已确认不管输入什么字符串都模糊匹配
                                    SaleOrderItem.barcode.ilike(
                                        keywords_search_ilike_str,
                                        escape=DEFAULT_ESCAPE_CHAR,
                                    )
                                )
                            )
                        )
                    )
                ),
            )
        # 添加门店ID条件
        if query_param_in.store_ids:
            conditions.append(
                SaleOrder.store_team_info_id.in_(query_param_in.store_ids)
            )
        # 添加渠道ID条件
        if query_param_in.channel_ids:
            conditions.append(SaleOrder.channel_id.in_(query_param_in.channel_ids))
        # 添加状态条件
        if query_param_in.states:
            conditions.append(SaleOrder.state.in_(query_param_in.states))
        else:
            conditions.append(SaleOrder.state.in_([4, 5, 6, 8, 9, 10, 11]))
        # 添加操作人搜索条件
        if query_param_in.operater_name_or_phone:
            conditions.append(
                or_(
                    SaleOrder.operater_name.ilike(
                        f"%{query_param_in.operater_name_or_phone}%"
                    ),
                    SaleOrder.operater_phone.ilike(
                        f"%{query_param_in.operater_name_or_phone}%"
                    ),
                    # 新增导购员
                    SaleOrder.shopping_guide_name.ilike(
                        f"%{query_param_in.operater_name_or_phone}%"
                    )
                )
            )
        # 添加创建时间范围条件
        if query_param_in.create_at_start:
            conditions.append(SaleOrder.created_at >= query_param_in.create_at_start)

        if query_param_in.create_at_end:
            conditions.append(SaleOrder.created_at <= query_param_in.create_at_end)
        # 添加营业日范围条件
        if query_param_in.business_day_start:
            conditions.append(
                SaleOrder.business_day >= query_param_in.business_day_start
            )

        if query_param_in.business_day_end:
            conditions.append(SaleOrder.business_day <= query_param_in.business_day_end)

        if query_param_in.payment_method:
            conditions.append(
                exists(
                    select(1)
                    .select_from(SaleOrderPayment)
                    .where(
                        and_(
                            SaleOrderPayment.order_id == SaleOrder.record_id,
                            SaleOrderPayment.payment_method_id.in_(
                                query_param_in.payment_method
                            ),
                            SaleOrderPayment.is_pay_success.is_(True),
                        )
                    )
                    .correlate(SaleOrder)  # 明确指定与 SaleOrder 的关联
                )
            )
        # 构建基础查询
        base_query = (
            select(SaleOrder.id).where(and_(*conditions)).cte("order_base_table")
        )

        # 支付成功数据子查询(SaleOrder.record_id == xxx.order_id )
        pay_success_query = (
            select(
                SaleOrder.id,
                func.round(
                    func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), 2
                ).label("success_pay_amount"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrderPayment.is_pay_success.is_(True),
                    SaleOrder.id.in_(select(base_query.c.id)),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_list_pay_success_data")
        payment_methods_subquery = (
            select(
                SaleOrderPayment.order_id,
                func.string_agg(
                    func.distinct(SaleOrderPayment.payment_method_name), "、"
                ).label("pay_channel"),
            )
            .select_from(SaleOrderPayment)
            .group_by(SaleOrderPayment.order_id)
        ).cte("payment_methods")
        # 主查询
        main_query = (
            select(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.store_name,
                SaleOrder.channel_name,
                func.concat(SaleOrder.store_name, " - ", SaleOrder.channel_name).label(
                    "store_channel_name"
                ),
                case(
                    (
                        and_(
                            SaleOrder.member_phone.isnot(None),
                            SaleOrder.member_name.isnot(None),
                            SaleOrder.member_name != "",
                            SaleOrder.member_phone != "",
                        ),
                        func.concat(
                            SaleOrder.member_name, "(", SaleOrder.member_phone, ")"
                        ),
                    ),
                    (
                        or_(
                            SaleOrder.member_name.is_(None),
                            SaleOrder.member_name == "",
                        ),
                        "散客",
                    ),
                    else_=SaleOrder.member_name,
                ).label("member_name_phone"),
                func.to_char(SaleOrder.create_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "create_at"
                ),
                case(
                    (
                        SaleOrder.total_origin_price.is_not(None),
                        cast(
                            func.round(cast(SaleOrder.total_origin_price, Numeric), 2),
                            String,
                        ),
                    ),
                    else_="-",
                ).label("total_origin_price"),
                case(
                    (
                        SaleOrder.discount_price.is_not(None),
                        cast(
                            func.round(cast(SaleOrder.discount_price, Numeric), 2),
                            String,
                        ),
                    ),
                    else_="-",
                ).label("discount_price"),
                case(
                    (state_case.in_(["已创建", "待支付"]), "-"),
                    (
                        pay_success_query.c.success_pay_amount.isnot(None),
                        cast(pay_success_query.c.success_pay_amount, String),
                    ),
                    else_="-",
                ).label("receive_price"),
                state_case.label("state_name"),
                case(
                    (SaleOrder.operater_name.isnot(None), SaleOrder.operater_name),
                    else_="-",
                ).label("operater_name_phone"),
                payment_methods_subquery.c.pay_channel,
            )
            .select_from(SaleOrder)
            .outerjoin(
                SaleOrderDiscount,
                and_(
                    SaleOrder.record_id == SaleOrderDiscount.order_id,
                    SaleOrderDiscount.disabled.is_(False),
                ),
            )
            .outerjoin(pay_success_query, SaleOrder.id == pay_success_query.c.id)
            .outerjoin(
                payment_methods_subquery,
                SaleOrder.record_id == payment_methods_subquery.c.order_id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.id.in_(select(base_query.c.id)),
                )
            )
            .group_by(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.store_name,
                SaleOrder.channel_name,
                SaleOrder.member_name,
                SaleOrder.member_phone,
                SaleOrder.create_at,
                SaleOrder.business_day,
                SaleOrder.total_origin_price,
                SaleOrder.discount_price,
                SaleOrder.receive_price,
                SaleOrder.operater_name,
                pay_success_query.c.success_pay_amount,
                SaleOrder.created_at,
                payment_methods_subquery.c.pay_channel,
            )
            .order_by(SaleOrder.created_at.desc())
            .limit(query_param_in.page_size)
            .offset(offset_count)
        )
        # 计算总数
        count_query = select(func.count()).select_from(
            select(SaleOrder.id)
            .where(SaleOrder.id.in_(select(base_query.c.id)))
            .subquery()
        )
        # 执行查询
        result = await self.db_session.execute(main_query)
        count_result = await self.db_session.execute(count_query)
        records = [dict(row._mapping) for row in result.fetchall()]

        order_numbers = list(map(lambda rd:rd.get("order_number"), records))

        order_items_info = await self.get_order_items_by_order_number(
            order_number_list=order_numbers,
        )

        for r in records:
            order_number = r.get("order_number")
            order_items = order_items_info.get(order_number, [])
            r["goods_info"] = order_items
            # 计算总购买数量
            total_purchase_quantity = self.get_total_purchase_quantity(
                order_items=order_items,
            )
            r["total_purchase_quantity"] = format_number_to_display(total_purchase_quantity)

        return {"records_list": records, "all_count": count_result.scalar()}

    @staticmethod
    def get_total_purchase_quantity(order_items: List[Dict[str,Any]]):
        total_purchase_quantity = Decimal("0")
        if not order_items:
            return total_purchase_quantity
        for order_item in order_items:
            purchase_quantity = order_item.get("purchase_quantity")
            if purchase_quantity:
                total_purchase_quantity += Decimal(purchase_quantity)

        return total_purchase_quantity

    async def get_order_items_by_order_number(
        self,
        order_number_list: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        根据订单号获取商品列表
        """
        if not order_number_list:
            return {}

        query_sql = """
            SELECT
                -- 订单号
                order_number,
                -- 商品销售名称
                goods_sale_name,
                -- 商品条码
                barcode,
                -- 商品自定义编码
                goods_custom_code,
                -- 购买数量
                purchase_quantity,
                -- 商品实收金额（销售金额）
                actual_receive_price,
                -- 商品零售优惠金额
                retail_discount_amount,
                -- 商品会员优惠金额
                member_discount_amount,
                -- 商品优惠总金额
                discount_amount_all,
                -- 商品成本价格
                costs,
                -- 会员价
                vip_price,
                -- 商品原价若存在改价则这里是改后价
                selling_price,
                -- 改价后商品售价
                discount_price_in_shopcar,
                -- 商品单位
                goods_unit_name,
                -- 商品图片
                picture_url,
                -- 商品分类名称
                category_name,
                -- 商品ID
                goods_id,
                -- 商品规格 1 单规格 2 多规格
                goods_spec,
                -- 商品SKU ID
                goods_package_sku_id,
                -- 商品规格信息
                goods_specification
            FROM 
                order_item
            WHERE 
                order_number = ANY (:order_number)
              AND disabled IS FALSE
            ORDER BY 
                id
        """
        stmt = text(query_sql)

        params = {
            "order_number": order_number_list,
        }
        result = await self.db_session.execute(
            stmt,
            params=params,
        )

        data = defaultdict(list)

        for row in result.mappings().all():
            current_order_number = row.get("order_number")
            data[current_order_number].append(row)

        return data


    async def do_get_local_order_detail_info(self, record_id: int, company_id: int):
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        # 支付成功数据子查询组装支付方式和金额
        pay_success_query = (
            select(
                SaleOrder.id,
                # 直接在子查询中构造完整的支付信息字符串
                func.array_agg(
                    text(
                        "ARRAY[CONCAT('(', payment_method_name, ')'), CONCAT('¥', CAST(payment_amount AS TEXT))] ORDER BY order_payment.sort ASC "
                    )
                ).label("payment_info"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
            .group_by(
                SaleOrder.id,
            )
        ).cte("orde_detail_pay_success_data")
        # 支付成功的查询，获取支付方式和金额
        pay_success_query_disperse = (
            select(
                SaleOrder.id,
                func.sum(cast(SaleOrderPayment.payment_amount, Numeric)).label(
                    "total_payment_amount"
                ),  # 在子查询中完成汇总
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)  # 按订单ID分组
        ).cte("orde_detail_pay_success_data_disperse")

        # 退款金额子查询
        refund_query = (
            select(
                SaleOrder.id,
                func.round(
                    func.sum(
                        func.coalesce(
                            cast(SaleOrderRefundPayment.refund_payment_amount, Numeric),
                            0,
                        )
                    ),
                    2,
                ).label("actually_refund_amount_all"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderReturn,
                SaleOrder.record_id == SaleOrderReturn.order_id,
                isouter=True,
            )
            .join(
                SaleOrderRefundPayment,
                SaleOrderReturn.record_id == SaleOrderRefundPayment.order_refund_id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderRefundPayment.is_refund_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_detail_refund_price")
        # 主查询
        main_query = (
            select(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.remark,
                case(
                    (SaleOrder.channel_name.is_(None), "--"),
                    else_=SaleOrder.channel_name,
                ).label("channel_name"),
                SaleOrder.store_name,
                func.concat(SaleOrder.store_name, " - ", SaleOrder.channel_name).label(
                    "store_channel_name"
                ),
                state_case.label("state_name"),
                case(
                    (
                        and_(
                            SaleOrder.member_phone.isnot(None),
                            SaleOrder.member_name.isnot(None),
                            SaleOrder.member_name != "",
                            SaleOrder.member_phone != "",
                        ),
                        func.concat(
                            SaleOrder.member_name, "(", SaleOrder.member_phone, ")"
                        ),
                    ),
                    (
                        or_(
                            SaleOrder.member_name.is_(None),
                            SaleOrder.member_name == "",
                        ),
                        "散客",
                    ),
                    else_=SaleOrder.member_name,
                ).label("member_name_phone"),
                func.to_char(SaleOrder.create_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "create_at"
                ),
                case(
                    (
                        SaleOrder.operater_name.isnot(None),
                        case(
                            (
                                SaleOrder.operater_phone.isnot(None),
                                func.concat(
                                    SaleOrder.operater_name,
                                    "(",
                                    SaleOrder.operater_phone,
                                    ")",
                                ),
                            ),
                            else_=SaleOrder.operater_name,
                        ),
                    ),
                    else_="-",
                ).label("operater_name_phone"),
                func.coalesce(
                    func.to_char(SaleOrder.paid_at, "YYYY-MM-DD HH24:MI:SS"), "-"
                ).label("paid_at"),
                case(
                    (
                        SaleOrder.total_origin_price.isnot(None),
                        func.concat(
                            "¥",
                            cast(
                                func.round(
                                    cast(SaleOrder.total_origin_price, Numeric), 2
                                ),
                                String,
                            ),
                        ),
                    ),
                    else_="-",
                ).label("total_origin_price"),
                case(
                    (
                        SaleOrder.discount_price.isnot(None),
                        func.concat(
                            "¥",
                            cast(
                                func.round(cast(SaleOrder.discount_price, Numeric), 2),
                                String,
                            ),
                        ),
                    ),
                    else_="-",
                ).label("discount_price"),
                pay_success_query.c.payment_info.label("pay_channel"),
                case(
                    (
                        refund_query.c.actually_refund_amount_all.is_not(None),
                        cast(refund_query.c.actually_refund_amount_all, String),
                    ),
                    else_="0.00",
                ).label("actually_refund_amount_all"),
                SaleOrder.shopping_guide_name,
                case(
                    (state_case.in_(["已创建", "待支付"]), "-"),
                    (
                        pay_success_query_disperse.c.total_payment_amount.isnot(
                            None
                        ),  # 使用已汇总的金额
                        func.concat(
                            "¥",
                            cast(
                                func.round(
                                    pay_success_query_disperse.c.total_payment_amount,  # 直接使用汇总后的金额
                                    2,
                                ),
                                String,
                            ),
                        ),
                    ),
                    else_="-",
                ).label("receive_price"),
            )
            .select_from(SaleOrder)
            .outerjoin(
                SaleOrderDiscount,
                and_(
                    SaleOrder.record_id == SaleOrderDiscount.order_id,
                    SaleOrderDiscount.disabled.is_(False),
                ),
            )
            .outerjoin(pay_success_query, SaleOrder.id == pay_success_query.c.id)
            .outerjoin(refund_query, SaleOrder.id == refund_query.c.id)
            .outerjoin(
                pay_success_query_disperse,
                SaleOrder.id == pay_success_query_disperse.c.id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.channel_name,
                SaleOrder.store_name,
                state_case,
                SaleOrder.member_name,
                SaleOrder.member_phone,
                SaleOrder.create_at,
                SaleOrder.operater_name,
                SaleOrder.operater_phone,
                SaleOrder.paid_at,
                SaleOrder.total_origin_price,
                SaleOrder.discount_price,
                refund_query.c.actually_refund_amount_all,
                SaleOrder.shopping_guide_name,
                pay_success_query.c.payment_info,
                pay_success_query_disperse.c.total_payment_amount,
            )
        )
        result = await self.db_session.execute(main_query)
        record = result.fetchone()
        return dict(record._mapping) if record else None

    async def do_get_order_pay_anomaly_info(self, record_id: int, company_id: int):
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        # 订单支付异常信息
        query = (
            select(
                SaleOrderPayment.payment_method_name,
                SaleOrderPayment.payment_amount,
                state_case.label("state_name"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(False),
                )
            )
        )

        result = await self.db_session.execute(query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records

    async def do_get_order_refund_base(self, record_id: int, company_id: int):
        """退款单信息"""
        # 支付成功金额CTE
        pay_success_price_query = (
            select(
                SaleOrder.id,
                func.sum(cast(SaleOrderPayment.payment_amount, Numeric)).label(
                    "payment_amount"
                ),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_detail_pay_success_price")
        # 子查询排序
        refund_payment_subquery = (
            select(
                SaleOrderRefundPayment.order_refund_id,
                literal_column(
                    """
                    array_agg(
                        CASE 
                            WHEN refund_payment_amount IS NOT NULL AND is_refund_success = true 
                            THEN ARRAY[
                                CONCAT('(', CAST(refund_payment_name AS TEXT), ')'),
                                CONCAT('¥', CAST(refund_payment_amount AS TEXT))
                            ]
                            WHEN refund_payment_amount IS NOT NULL AND (is_refund_success = false OR is_refund_success IS NULL) 
                            THEN ARRAY[
                                CONCAT('(', CAST(refund_payment_name AS TEXT), ')'),
                                '¥0.00'
                            ]
                            ELSE ARRAY[]::text[] 
                        END
                        ORDER BY sort ASC
                    )
                    """,
                    type_=ARRAY(String),  # 修改类型为数组
                ).label("refund_pay_channel"),
            )
            .select_from(SaleOrderRefundPayment)
            .where(
                and_(
                    SaleOrderRefundPayment.company_id == company_id,  # 根据公司ID过滤
                )
            )
            .group_by(SaleOrderRefundPayment.order_refund_id)
            .subquery()
        )

        # 主查询
        main_query = (
            select(
                func.to_char(SaleOrderReturn.create_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "create_at"
                ),
                SaleOrderReturn.refund_type_alias,
                cast(
                    func.round(
                        func.sum(
                            case(
                                (
                                    SaleOrderRefundPayment.is_refund_success.is_(True),
                                    cast(
                                        SaleOrderRefundPayment.refund_payment_amount,
                                        Numeric,
                                    ),
                                ),
                                else_=0,
                            )
                        ),
                        2,
                    ),
                    String,
                ).label("actually_refund_amount"),
                case(
                    (
                        SaleOrderReturn.operater_name.isnot(None),
                        case(
                            (
                                SaleOrderReturn.operater_phone.isnot(None),
                                func.concat(
                                    SaleOrderReturn.operater_name,
                                    "(",
                                    SaleOrderReturn.operater_phone,
                                    ")",
                                ),
                            ),
                            else_=SaleOrderReturn.operater_name,
                        ),
                    ),
                    else_="-",
                ).label("operater_name_phone"),
                SaleOrderReturn.refund_number,
                SaleOrderReturn.refund_reason,
                case(
                    (
                        func.max(SaleOrderRefundPayment.refund_success_time).isnot(
                            None
                        ),
                        func.to_char(
                            func.max(SaleOrderRefundPayment.refund_success_time),
                            "YYYY-MM-DD HH24:MI:SS",
                        ),
                    ),
                    else_="-",
                ).label("refund_success_time"),
                refund_payment_subquery.c.refund_pay_channel.label(
                    "refund_pay_channel"
                ),
            )
            .select_from(SaleOrder)
            .join(SaleOrderReturn, SaleOrder.record_id == SaleOrderReturn.order_id)
            .outerjoin(
                SaleOrderRefundPayment,
                SaleOrderReturn.record_id == SaleOrderRefundPayment.order_refund_id,
            )
            .outerjoin(
                SaleOrderReturnItem,
                SaleOrderReturn.record_id == SaleOrderReturnItem.order_refund_id,
            )
            .outerjoin(
                pay_success_price_query,
                SaleOrderReturn.record_id == pay_success_price_query.c.id,
            )
            .outerjoin(
                refund_payment_subquery,
                SaleOrderReturn.record_id == refund_payment_subquery.c.order_refund_id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(
                SaleOrderReturn.create_at,
                SaleOrderReturn.refund_type_alias,
                SaleOrderReturn.actually_refund_amount,
                SaleOrderReturn.operater_name,
                SaleOrderReturn.operater_phone,
                SaleOrderReturn.refund_number,
                pay_success_price_query.c.payment_amount,
                refund_payment_subquery.c.refund_pay_channel,
                SaleOrder.state,
                SaleOrderReturn.refund_reason,
            )
            .order_by(SaleOrderReturn.create_at.desc())
        )

        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records

    async def do_get_order_detail_items(self, record_id: int, company_id: int):
        """订单详情商品信息"""
        # 退款表数据CTE
        return_table_query = (
            select(
                SaleOrderItem.id,
                func.sum(cast(SaleOrderReturnItem.refund_quantity, Numeric)).label(
                    "refund_quantity"
                ),
            )
            .select_from(SaleOrderReturnItem)
            .join(
                SaleOrderItem,
                SaleOrderItem.record_id == SaleOrderReturnItem.order_item_id,
            )
            .join(SaleOrder, SaleOrder.record_id == SaleOrderItem.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrderReturnItem.is_refund_success.is_(True),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(SaleOrderItem.id)
        ).cte("return_table_data")
        # 主查询
        main_query = (
            select(
                SaleOrderItem.id,
                SaleOrderItem.spu_code,
                case(
                    (
                        and_(
                            return_table_query.c.refund_quantity.is_not(None),
                            SaleOrder.state == OrderStateEnum.PartRefunded.value,
                        ),
                        return_table_query.c.refund_quantity,
                    ),
                    else_=None,
                ).label("return_quantity"),
                SaleOrderItem.goods_sale_name,
                SaleOrderItem.goods_unit_name,
                case(
                    (SaleOrderItem.picture_url.is_not(None), SaleOrderItem.picture_url),
                    else_=None,
                ).label("picture_url"),
                SaleOrderItem.selling_price,
                cast(SaleOrderItem.extra, JSONB).label("extra"),
                case(
                    (
                        and_(
                            SaleOrderItem.discount_price_in_shopcar.isnot(None),
                            SaleOrderItem.discount_price_in_shopcar != "",
                        ),
                        True,
                    ),
                    else_=False,
                ).label("change_price_mark"),
                SaleOrderItem.discount_price_in_shopcar,
                SaleOrderItem.purchase_quantity,
                # 原小计
                func.concat(
                    "￥",
                    func.round(
                        cast(SaleOrderItem.selling_price, Numeric)
                        * cast(SaleOrderItem.purchase_quantity, Numeric),
                        2,
                    ),
                ).label("total_price_item"),
                SaleOrderItem.actual_receive_price,
                case(
                    (
                        and_(
                            return_table_query.c.refund_quantity.isnot(None),
                            cast(SaleOrderItem.purchase_quantity, Numeric)
                            == return_table_query.c.refund_quantity,
                        ),
                        "已退款",
                    ),
                    (return_table_query.c.refund_quantity.isnot(None), "部分退款"),
                    else_="",
                ).label("refund_status"),

                # goods_spec
                SaleOrderItem.goods_spec,
                # goods_package_sku_id
                SaleOrderItem.goods_package_sku_id,
                # sku_code
                SaleOrderItem.sku_code,
                # goods_specification
                SaleOrderItem.goods_specification,
            )
            .select_from(SaleOrder)
            .join(SaleOrderItem, SaleOrder.record_id == SaleOrderItem.order_id)
            .outerjoin(return_table_query, return_table_query.c.id == SaleOrderItem.id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
        )

        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records

    async def do_get_order_discount_detail_price(self, record_id: int, company_id: int):
        """订单详情中折扣优惠信息"""
        # 系统抹零折扣CTE
        system_discount_query = (
            select(
                func.cast("系统抹零", String).label("name"),
                cast(
                    func.round(
                        func.sum(cast(SaleOrderDiscount.discount_amount, Numeric)), 2
                    ),
                    String,
                ).label("amount"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderDiscount, SaleOrder.record_id == SaleOrderDiscount.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderDiscount.discount_source == 1,
                )
            )
            .group_by(SaleOrderDiscount.discount_source)
        ).cte("system_discount_data")
        # 其他折扣CTE
        other_discount_query = (
            select(
                SaleOrderDiscount.discount_name.label("name"),
                cast(
                    func.round(
                        func.sum(cast(SaleOrderDiscount.discount_amount, Numeric)), 2
                    ),
                    String,
                ).label("amount"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderDiscount, SaleOrder.record_id == SaleOrderDiscount.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderDiscount.discount_source != 1,
                )
            )
            .group_by(SaleOrderDiscount.discount_name)
        ).cte("other_discount_data")

        # Union查询
        union_query = union_all(
            select(system_discount_query), select(other_discount_query)
        )

        result = await self.db_session.execute(union_query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records

    async def do_get_order_state_info(self, company_id: int):
        state_case = self.sale_order_state_trans(
            exclude_state=[0, 1, 2, 3, 7, 12, 13, 14, 15, 16, 17]
        )
        query = (
            select(
                SaleOrder.state.label("id"),
                SaleOrder.state.label("value"),
                # 使用 case 语句来映射状态名称
                state_case.label("label"),
                # name 和 label 相同
                state_case.label("name"),
            )
            .select_from(SaleOrder)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    state_case.not_in(["未知状态"]),
                )
            )
            .group_by(SaleOrder.state)
            .order_by(SaleOrder.state)
        )

        result = await self.db_session.execute(query)
        return [dict(row._mapping) for row in result.fetchall()]

    async def do_get_order_payment_method_info(self, company_id: int):
        """订单支付方式"""
        query_payment_method = (
            select(
                SaleOrderPayment.payment_method_id.label("id"),
                SaleOrderPayment.payment_method_name.label("name"),
                SaleOrderPayment.payment_method_name.label("label"),
                SaleOrderPayment.payment_method_id.label("value"),
            )
            .distinct()
            .where(SaleOrderPayment.company_id == company_id)
        )
        result = await self.db_session.execute(query_payment_method)
        return [dict(row._mapping) for row in result.fetchall()]

    def get_amount_condition(self, amount_filter: AmountFilter):
        """
        构建金额查询条件
        """
        # 定义金额字段映射
        amount_field_mapping = {
            "total_price": SaleOrder.total_origin_price,
            "discount_price": SaleOrder.discount_price,
            "receive_price": SaleOrderPayment.payment_amount,  # 这个需要特殊处理
        }
        field = amount_field_mapping.get(amount_filter.amount_type)
        if not field:
            return None
        # 如果是实付金额，需要特殊处理
        if amount_filter.amount_type == "receive_price":
            subquery = (
                select(func.sum(cast(SaleOrderPayment.payment_amount, Numeric)))
                .where(
                    and_(
                        SaleOrderPayment.order_id == SaleOrder.record_id,
                        SaleOrderPayment.is_pay_success.is_(True),
                    )
                )
                .correlate(SaleOrder)
            ).scalar_subquery()
            field = subquery
        # 转换金额为 Numeric 类型
        value = cast(amount_filter.value, Numeric)
        # 根据操作符构建条件
        operator_mapping = {
            ComparisonOperator.GT: lambda f, v: cast(f, Numeric) > v,
            ComparisonOperator.GTE: lambda f, v: cast(f, Numeric) >= v,
            ComparisonOperator.LT: lambda f, v: cast(f, Numeric) < v,
            ComparisonOperator.LTE: lambda f, v: cast(f, Numeric) <= v,
            ComparisonOperator.EQ: lambda f, v: cast(f, Numeric) == v,
            ComparisonOperator.NEQ: lambda f, v: cast(f, Numeric) != v,
        }
        return operator_mapping[amount_filter.operator](field, value)

    async def do_get_local_order_pc_list_info(self, query_param_in: QueryParamPCIn):
        """优化后的分页获取本地订单列表(PC端)"""
        state_case = self.sale_order_state_trans()

        offset_count = query_param_in.page_size * (query_param_in.page_number - 1)

        conditions = [
            SaleOrder.disabled.is_(False),
            SaleOrder.company_id == query_param_in.company_id,
            SaleOrder.order_source == OrderSourceEnum.STORE_ORDER.code,
        ]

        if query_param_in.store_ids:
            conditions.append(SaleOrder.store_team_info_id.in_(query_param_in.store_ids))

        if query_param_in.order_number:
            conditions.append(SaleOrder.order_number.ilike(f"%{query_param_in.order_number}%"))

        if query_param_in.product_name:
            product_name_like_str = fuzzy_search_string(
                string=query_param_in.product_name,
                escape_char=DEFAULT_ESCAPE_CHAR,
            )
            conditions.append(
                exists(
                    select(1)
                    .select_from(SaleOrderItem)
                    .where(
                        and_(
                            SaleOrderItem.order_id == SaleOrder.record_id,
                            SaleOrderItem.disabled.is_(False),
                            or_(
                                SaleOrderItem.goods_sale_name.ilike(
                                    product_name_like_str,
                                    escape=DEFAULT_ESCAPE_CHAR,
                                ),
                                SaleOrderItem.barcode.ilike(
                                    product_name_like_str,
                                    escape=DEFAULT_ESCAPE_CHAR,
                                ),
                            ),
                        )
                    )
                    .correlate(SaleOrder)
                )
            )

        if query_param_in.states:
            conditions.append(SaleOrder.state.in_(query_param_in.states))
        else:
            conditions.append(SaleOrder.state.in_([4, 5, 6, 8, 9, 10, 11]))

        if query_param_in.amount_filter:
            for amount_filter in query_param_in.amount_filter:
                condition = self.get_amount_condition(amount_filter)
                if condition is not None:
                    conditions.append(condition)

        if query_param_in.payment_method:
            conditions.append(
                exists(
                    select(1)
                    .select_from(SaleOrderPayment)
                    .where(
                        and_(
                            SaleOrderPayment.order_id == SaleOrder.record_id,
                            SaleOrderPayment.payment_method_id.in_(query_param_in.payment_method),
                            SaleOrderPayment.is_pay_success.is_(True),
                        )
                    )
                    .correlate(SaleOrder)
                )
            )

        if query_param_in.create_at_start:
            conditions.append(SaleOrder.created_at >= query_param_in.create_at_start)
        if query_param_in.create_at_end:
            conditions.append(SaleOrder.created_at <= query_param_in.create_at_end)

        if query_param_in.channel_ids:
            conditions.append(SaleOrder.channel_id.in_(query_param_in.channel_ids))

        if query_param_in.user_name_or_phone:
            conditions.append(
                or_(
                    SaleOrder.member_name.ilike(f"%{query_param_in.user_name_or_phone}%"),
                    SaleOrder.member_phone.ilike(f"%{query_param_in.user_name_or_phone}%"),
                )
            )

        if query_param_in.operater_name_or_phone:
            conditions.append(
                or_(
                    SaleOrder.operater_name.ilike(f"%{query_param_in.operater_name_or_phone}%"),
                    SaleOrder.operater_phone.ilike(f"%{query_param_in.operater_name_or_phone}%"),
                )
            )

        if query_param_in.business_day_start:
            conditions.append(SaleOrder.business_day >= query_param_in.business_day_start)
        if query_param_in.business_day_end:
            conditions.append(SaleOrder.business_day <= query_param_in.business_day_end)

        base_query = (
            select(SaleOrder.id, SaleOrder.order_number)
            .where(and_(*conditions))
            .cte("base_order")
        )

        pay_amount_sub = (
            select(
                SaleOrder.id.label("id"),
                func.round(func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), 2).label(
                    "success_pay_amount"
                ),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(
                    SaleOrderPayment.is_pay_success.is_(True),
                    SaleOrder.id.in_(select(base_query.c.id)),
                )
            )
            .group_by(SaleOrder.id)
            .cte("pay_amount_sub")
        )

        order_query = select(base_query.c.id, base_query.c.order_number).select_from(base_query)

        if query_param_in.receive_price:
            order_query = order_query.outerjoin(pay_amount_sub, base_query.c.id == pay_amount_sub.c.id)
            if query_param_in.receive_price == "descend":
                order_query = order_query.order_by(pay_amount_sub.c.success_pay_amount.desc())
            else:
                order_query = order_query.order_by(pay_amount_sub.c.success_pay_amount.asc())
        elif query_param_in.total_origin_price:
            if query_param_in.total_origin_price == "descend":
                order_query = order_query.order_by(SaleOrder.total_origin_price.desc())
            else:
                order_query = order_query.order_by(SaleOrder.total_origin_price.asc())
        elif query_param_in.discount_price:
            if query_param_in.discount_price == "descend":
                order_query = order_query.order_by(SaleOrder.discount_price.desc())
            else:
                order_query = order_query.order_by(SaleOrder.discount_price.asc())
        else:
            order_query = order_query.order_by(SaleOrder.created_at.desc())

        order_query = order_query.limit(query_param_in.page_size).offset(offset_count)

        id_result = await self.db_session.execute(order_query)
        id_rows = id_result.fetchall()
        order_ids = [r.id for r in id_rows]
        order_numbers = [r.order_number for r in id_rows]

        if not order_ids:
            return {
                "records_list": [],
                "amount_data": {
                    "total_price": 0,
                    "total_discount_price": 0,
                    "total_receive_price": 0,
                    "total_count": 0,
                },
            }

        pay_success_query = (
            select(
                SaleOrder.id,
                func.round(func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), 2).label(
                    "success_pay_amount"
                ),
            )
            .select_from(SaleOrder)
            .join(SaleOrderPayment, SaleOrder.record_id == SaleOrderPayment.order_id)
            .where(
                and_(SaleOrderPayment.is_pay_success.is_(True), SaleOrder.id.in_(order_ids))
            )
            .group_by(SaleOrder.id)
        ).cte("pay_success_query")

        payment_methods_subquery = (
            select(
                SaleOrderPayment.order_id,
                func.string_agg(func.distinct(SaleOrderPayment.payment_method_name), "、").label("pay_channel"),
            )
            .select_from(SaleOrderPayment)
            .where(SaleOrderPayment.order_id.in_(order_ids))
            .group_by(SaleOrderPayment.order_id)
        ).cte("payment_methods")

        main_query = (
            select(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.store_name,
                SaleOrder.channel_name,
                func.concat(SaleOrder.store_name, " - ", SaleOrder.channel_name).label("store_channel_name"),
                case(
                    (
                        and_(
                            SaleOrder.member_phone.isnot(None),
                            SaleOrder.member_name.isnot(None),
                            SaleOrder.member_name != "",
                            SaleOrder.member_phone != "",
                        ),
                        func.concat(SaleOrder.member_name, "(", SaleOrder.member_phone, ")"),
                    ),
                    (
                        or_(SaleOrder.member_name.is_(None), SaleOrder.member_name == ""),
                        "散客",
                    ),
                    else_=SaleOrder.member_name,
                ).label("member_name_phone"),
                func.to_char(SaleOrder.create_at, "YYYY-MM-DD HH24:MI:SS").label("create_at"),
                case(
                    (
                        SaleOrder.total_origin_price.is_not(None),
                        cast(func.round(cast(SaleOrder.total_origin_price, Numeric), 2), String),
                    ),
                    else_="-",
                ).label("total_origin_price"),
                case(
                    (
                        SaleOrder.discount_price.is_not(None),
                        cast(func.round(cast(SaleOrder.discount_price, Numeric), 2), String),
                    ),
                    else_="-",
                ).label("discount_price"),
                case(
                    (state_case.in_(["已创建", "待支付"]), "-"),
                    (
                        pay_success_query.c.success_pay_amount.isnot(None),
                        cast(pay_success_query.c.success_pay_amount, String),
                    ),
                    else_="-",
                ).label("receive_price"),
                state_case.label("state_name"),
                case(
                    (SaleOrder.operater_name.isnot(None), SaleOrder.operater_name),
                    else_="-",
                ).label("operater_name_phone"),
                payment_methods_subquery.c.pay_channel,
            )
            .select_from(SaleOrder)
            .outerjoin(pay_success_query, SaleOrder.id == pay_success_query.c.id)
            .outerjoin(payment_methods_subquery, SaleOrder.record_id == payment_methods_subquery.c.order_id)
            .where(SaleOrder.id.in_(order_ids))
        )

        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]

        order_items_info = await self.get_order_items_by_order_number(order_numbers)

        for r in records:
            order_number = r.get("order_number")
            order_items = order_items_info.get(order_number, [])
            r["goods_info"] = order_items
            total_purchase_quantity = self.get_total_purchase_quantity(order_items=order_items)
            r["total_purchase_quantity"] = format_number_to_display(total_purchase_quantity)

        amount_count_query = (
            select(
                func.coalesce(func.round(func.sum(cast(SaleOrder.total_origin_price, Numeric)), 2), 0).label("total_price"),
                func.coalesce(func.round(func.sum(cast(SaleOrder.discount_price, Numeric)), 2), 0).label("total_discount_price"),
                func.coalesce(func.round(func.sum(cast(pay_amount_sub.c.success_pay_amount, Numeric)), 2), 0).label("total_receive_price"),
                func.count(SaleOrder.id).label("total_count"),
            )
            .select_from(SaleOrder)
            .outerjoin(pay_amount_sub, SaleOrder.id == pay_amount_sub.c.id)
            .where(SaleOrder.id.in_(select(base_query.c.id)))
        )
        amount_count_result = await self.db_session.execute(amount_count_query)
        amount_data = dict(amount_count_result.fetchone() or {})

        return {"records_list": records, "amount_data": amount_data}

    async def get_last_refund_payment_agg_pay_info(
        self, record_id: int, company_id: int
    ):
        """获取该订单的最后一笔退款单（聚合支付）[ SaleOrder.record_id == SaleOrderReturn.order_id]"""
        last_refund_payment_agg_pay = (
            select(
                SaleOrderRefundPayment.is_refund_success,
                SaleOrderRefundPayment.is_pre_refund_success,
                SaleOrder.id.label("order_id"),
                SaleOrderReturn.id.label("order_refund_id"),
            )
            .select_from(SaleOrderReturn)
            .join(SaleOrder, SaleOrder.record_id == SaleOrderReturn.order_id)
            .join(
                SaleOrderRefundPayment,
                SaleOrderReturn.record_id == SaleOrderRefundPayment.order_refund_id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    # 只需要选择聚合支付
                    SaleOrderRefundPayment.refund_payment_method_id == -1,
                )
            )
            # 按id降序排序
            .order_by(SaleOrderReturn.id.desc())
            .limit(1)
        )
        last_refund_payment_agg_pay_result = await self.db_session.execute(
            last_refund_payment_agg_pay
        )
        last_refund_payment_agg_pay_record = (
            last_refund_payment_agg_pay_result.fetchone()
        )
        return (
            dict(last_refund_payment_agg_pay_record._mapping)
            if last_refund_payment_agg_pay_record
            else None
        )

    async def do_get_local_order_detail_pc_info(self, record_id: int, company_id: int):
        """
        获取订单详情基础信息
        """
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        # 获取订单来源
        order_source_case = self.order_source_trans()
        pay_success_data = (
            select(
                SaleOrder.id,
                SaleOrderPayment.payment_amount,
                SaleOrderPayment.payment_method_name,
                SaleOrderPayment.sort,
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
        ).cte("orde_detail_pay_success_data")

        pay_success_total_amount = (
            select(
                SaleOrder.id,
                cast(
                    func.round(
                        func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), 2
                    ),
                    String,
                ).label("total_amount"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("pay_success_total_amount")

        base_data = (
            select(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.change_money,
                SaleOrder.shopping_guide_name,
                order_source_case.label("order_source_name"),
                case(
                    (SaleOrder.channel_name.is_(None), "--"),
                    else_=SaleOrder.channel_name,
                ).label("channel_name"),
                SaleOrder.store_name,
                SaleOrder.remark,
                case(
                    (
                        and_(
                            SaleOrder.member_phone.isnot(None),
                            SaleOrder.member_name.isnot(None),
                            SaleOrder.member_name != "",
                            SaleOrder.member_phone != "",
                        ),
                        func.concat(
                            SaleOrder.member_name, "(", SaleOrder.member_phone, ")"
                        ),
                    ),
                    (
                        or_(
                            SaleOrder.member_name.is_(None),
                            SaleOrder.member_name == "",
                        ),
                        "散客",
                    ),
                    else_=SaleOrder.member_name,
                ).label("member_name_phone"),
                state_case.label("state_name"),
                func.to_char(SaleOrder.create_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "create_at"
                ),
                case(
                    (
                        SaleOrder.operater_name.isnot(None),
                        case(
                            (
                                SaleOrder.operater_phone.isnot(None),
                                func.concat(
                                    SaleOrder.operater_name,
                                    "(",
                                    SaleOrder.operater_phone,
                                    ")",
                                ),
                            ),
                            else_=SaleOrder.operater_name,
                        ),
                    ),
                    else_="_",
                ).label("operater_name_phone"),
                func.to_char(SaleOrder.paid_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "paid_at"
                ),
                case(
                    (
                        and_(
                            SaleOrder.total_origin_price.isnot(None),
                            SaleOrder.total_origin_price != "0",
                        ),
                        func.concat(
                            "¥",
                            func.cast(
                                func.round(
                                    func.cast(SaleOrder.total_origin_price, Numeric), 2
                                ),
                                String,
                            ),
                        ),
                    ),
                    else_="-",
                ).label("total_origin_price"),
                case(
                    (
                        and_(
                            SaleOrder.discount_price.isnot(None),
                            SaleOrder.discount_price != "0",
                        ),
                        func.concat(
                            "¥",
                            func.cast(
                                func.round(
                                    func.cast(SaleOrder.discount_price, Numeric), 2
                                ),
                                String,
                            ),
                        ),
                    ),
                    else_="-",
                ).label("discount_price"),
                func.concat(
                    "¥",
                    func.cast(
                        func.round(func.cast(SaleOrder.origin_price, Numeric), 2),
                        String,
                    ),
                ).label("origin_price"),
                case(
                    (func.max(pay_success_data.c.payment_amount).is_(None), "-"),
                    else_=func.string_agg(
                        case(
                            (
                                or_(state_case == "支付异常", state_case == "支付失败"),
                                func.concat(
                                    "¥",
                                    "0.00",
                                    "(",
                                    pay_success_data.c.payment_method_name,
                                    ")",
                                ),
                            ),
                            else_=func.concat(
                                "¥",
                                pay_success_data.c.payment_amount,
                                "(",
                                pay_success_data.c.payment_method_name,
                                ")",
                            ),
                        ),
                        "; ",
                    ).over(order_by=pay_success_data.c.sort.asc()),
                ).label("pay_channel"),
                pay_success_total_amount.c.total_amount.label("success_pay_amount"),
            )
            .select_from(SaleOrder)
            .join(pay_success_data, SaleOrder.id == pay_success_data.c.id, isouter=True)
            .join(
                pay_success_total_amount,
                SaleOrder.id == pay_success_total_amount.c.id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(
                SaleOrder.id,
                SaleOrder.order_number,
                SaleOrder.store_name,
                SaleOrder.channel_name,
                SaleOrder.member_name,
                SaleOrder.member_phone,
                SaleOrder.operater_name,
                SaleOrder.operater_phone,
                SaleOrder.create_at,
                SaleOrder.paid_at,
                SaleOrder.total_origin_price,
                SaleOrder.discount_price,
                pay_success_data.c.payment_amount,
                pay_success_data.c.payment_method_name,
                pay_success_data.c.sort,
                pay_success_total_amount.c.total_amount,
            )
        ).cte("orde_detail_base_data")
        # 获取订单详情
        main_query = select(
            base_data.c.order_number,
            base_data.c.channel_name,
            base_data.c.store_name,
            base_data.c.state_name,
            base_data.c.shopping_guide_name,
            base_data.c.member_name_phone,
            base_data.c.create_at,
            base_data.c.operater_name_phone,
            base_data.c.remark,
            # 支付时间
            func.coalesce(base_data.c.paid_at, "-").label("paid_at"),
            # 订单金额
            base_data.c.total_origin_price,
            # 优惠金额
            base_data.c.discount_price,
            # 销售金额
            base_data.c.origin_price,
            # 付款方式
            base_data.c.pay_channel,
            base_data.c.order_source_name,
            # 新增字段退款状态【aggregated_refund_result_code】
            # 实付金额
            case(
                (base_data.c.state_name.in_(["已创建", "待支付"]), "-"),
                (
                    base_data.c.success_pay_amount.isnot(None),
                    func.concat("¥", base_data.c.success_pay_amount),
                ),
                else_="-",
            ).label("receive_price"),
        ).select_from(base_data)
        # 执行查询
        result = await self.db_session.execute(main_query)
        record = result.fetchone()
        return dict(record._mapping) if record else None

    async def do_get_order_pc_refund_base(self, record_id: int, company_id: int):
        """
        获取订单退款基础信息
        """
        # 获取退款订单商品信息
        refund_item_query = (
            select(
                # 退款单id
                SaleOrderReturnItem.order_refund_id,
                # 退款项id
                SaleOrderReturnItem.id,
                # 商品名称
                SaleOrderItem.goods_sale_name,
                # 商品条码
                SaleOrderItem.spu_code,
                # 商品单位
                SaleOrderItem.goods_unit_name,
                # 商品售价
                SaleOrderItem.selling_price,
                # 购买数量
                SaleOrderItem.purchase_quantity,
                # 销售金额
                func.concat("￥", SaleOrderItem.actual_receive_price).label(
                    "actual_receive_price"
                ),
                SaleOrderItem.actual_receive_price.label(
                    "actual_receive_price_no_symbol"
                ),
                # 退款数量
                SaleOrderReturnItem.refund_quantity,
                # 退款金额
                func.concat("￥", SaleOrderReturnItem.refund_price).label(
                    "refund_price"
                ),
                SaleOrderReturnItem.refund_price.label("refund_price_no_symbol"),
                # 商品图片
                SaleOrderItem.picture_url,
                # 商品条码
                SaleOrderItem.barcode,
                # 商品ID
                SaleOrderItem.goods_id,
                # 商品规格 1 单规格 2 多规格
                SaleOrderItem.goods_spec,
                # 商品SKU ID
                SaleOrderItem.goods_package_sku_id,
                # 商品规格信息
                SaleOrderItem.goods_specification,
            )
            #  SaleOrderItem.record_id == SaleOrderReturnItem.order_item_id,
            .select_from(SaleOrderReturnItem)
            .join(
                SaleOrderItem,
                SaleOrderItem.record_id == SaleOrderReturnItem.order_item_id,
            )
            .join(SaleOrder, SaleOrder.record_id == SaleOrderItem.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
        )
        refund_payment_info = (
            select(
                SaleOrderRefundPayment.order_refund_id,
                case(
                    (
                        func.max(SaleOrderRefundPayment.refund_success_time).isnot(
                            None
                        ),
                        func.to_char(
                            func.max(SaleOrderRefundPayment.refund_success_time),
                            "YYYY-MM-DD HH24:MI:SS",
                        ),
                    ),
                    else_="-",
                ).label("refund_success_time"),
                literal_column(
                    """
                    string_agg(
                        CASE 
                            WHEN refund_payment_amount IS NOT NULL AND is_refund_success = true 
                            THEN CONCAT('¥', refund_payment_amount, '(', refund_payment_name, ')')
                            WHEN refund_payment_amount IS NOT NULL AND (is_refund_success = false OR is_refund_success IS NULL) 
                            THEN CONCAT('¥', '0.00', '(', refund_payment_name, ')')
                            ELSE '-'
                        END,
                        '; ' 
                        ORDER BY sort
                    )
                """,
                    type_=String,
                ).label("refund_pay_channel"),
                func.sum(
                    case(
                        (
                            SaleOrderRefundPayment.is_refund_success.is_(True),
                            cast(SaleOrderRefundPayment.refund_payment_amount, Numeric),
                        ),
                        else_=0,
                    )
                ).label("refund_payment_amount"),
            )
            .select_from(SaleOrderRefundPayment)
            .join(
                SaleOrderReturn,
                SaleOrderReturn.record_id == SaleOrderRefundPayment.order_refund_id,
            )
            .join(SaleOrder, SaleOrder.record_id == SaleOrderReturn.order_id)
            .where(
                and_(
                    SaleOrderRefundPayment.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(SaleOrderRefundPayment.order_refund_id)
            .subquery()
        )
        main_query = (
            select(
                func.to_char(SaleOrderReturn.create_at, "YYYY-MM-DD HH24:MI:SS").label(
                    "create_at"
                ),
                SaleOrderReturn.refund_type_alias,
                func.concat(
                    "￥",
                    func.cast(
                        func.round(
                            func.sum(
                                case(
                                    (
                                        refund_payment_info.c.refund_payment_amount.is_not(
                                            None
                                        ),
                                        func.cast(
                                            refund_payment_info.c.refund_payment_amount,
                                            Numeric,
                                        ),
                                    ),
                                    else_=0,
                                )
                            ),
                            2,
                        ),
                        String,
                    ),
                ).label("actually_refund_amount"),
                case(
                    (
                        SaleOrderReturn.operater_name.isnot(None),
                        case(
                            (
                                SaleOrderReturn.operater_phone.isnot(None),
                                func.concat(
                                    SaleOrderReturn.operater_name,
                                    "(",
                                    SaleOrderReturn.operater_phone,
                                    ")",
                                ),
                            ),
                            else_=SaleOrderReturn.operater_name,
                        ),
                    ),
                    else_="_",
                ).label("operater_name_phone"),
                SaleOrderReturn.refund_number,
                SaleOrderReturn.refund_reason,
                SaleOrderReturn.id.label("refund_record_id"),
                SaleOrderReturn.record_id,
                case(
                    (
                        refund_payment_info.c.refund_success_time.is_not(None),
                        refund_payment_info.c.refund_success_time,
                    ),
                    else_="-",
                ).label("refund_success_time"),
                refund_payment_info.c.refund_pay_channel,
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderReturn,
                SaleOrder.record_id == SaleOrderReturn.order_id,
                isouter=True,
            )
            .join(
                refund_payment_info,
                SaleOrderReturn.record_id == refund_payment_info.c.order_refund_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(
                SaleOrderReturn.id,
                SaleOrderReturn.create_at,
                SaleOrderReturn.refund_type_alias,
                SaleOrderReturn.operater_name,
                SaleOrderReturn.operater_phone,
                SaleOrderReturn.refund_number,
                SaleOrder.state,
                SaleOrderReturn.refund_reason,
                refund_payment_info.c.refund_success_time,
                refund_payment_info.c.refund_pay_channel,
            )
            .order_by(SaleOrderReturn.create_at.desc())
        )
        # 执行查询
        refund = await self.db_session.execute(main_query)
        refund_item = await self.db_session.execute(refund_item_query)
        refunds = [dict(row._mapping) for row in refund.fetchall()]
        refund_items = [dict(row._mapping) for row in refund_item.fetchall()]
        return {
            "refunds": refunds or [],
            "refund_items": refund_items or [],
        }

    async def do_get_order_pay_anomaly_pc_info(self, record_id: int, company_id: int):
        """
        获取订单支付异常信息
        """
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        main_query = (
            select(
                SaleOrderPayment.payment_method_name,
                literal_column("0.00").label("payment_amount"),
                state_case.label("state_name"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(False),
                )
            )
        )
        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records

    async def do_get_order_detail_pc_items(self, record_id: int, company_id: int):
        return_table_data = (
            select(
                SaleOrderItem.id,
                func.sum(cast(SaleOrderReturnItem.refund_quantity, Numeric)).label(
                    "refund_quantity"
                ),
            )
            .select_from(SaleOrderReturnItem)
            .join(
                SaleOrderItem,
                SaleOrderItem.record_id == SaleOrderReturnItem.order_item_id,
                isouter=True,
            )
            .join(
                SaleOrder, SaleOrder.record_id == SaleOrderItem.order_id, isouter=True
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrderReturnItem.is_refund_success.is_(True),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(SaleOrderItem.id)
        ).cte("return_table_data")

        main_query = (
            select(
                SaleOrderItem.spu_code,
                SaleOrderItem.goods_sale_name,
                SaleOrderItem.goods_unit_name,
                case(
                    (SaleOrderItem.picture_url.is_not(None), SaleOrderItem.picture_url),
                    else_=None,
                ).label("picture_url"),
                # 售价
                func.concat("￥", SaleOrderItem.selling_price).label("selling_price"),
                # 改后价
                case(
                    (
                        SaleOrderItem.discount_price_in_shopcar.is_not(None),
                        func.concat("￥", SaleOrderItem.discount_price_in_shopcar),
                    ),
                    else_=SaleOrderItem.discount_price_in_shopcar,
                ).label("discount_price_in_shopcar"),
                # 特价信息
                func.jsonb_extract_path_text(
                    cast(SaleOrderItem.extra, JSONB),
                    "limit_time_special",
                    "discount_price",
                ).label("discount_price"),

                # goods_discounts 商品折扣
                func.jsonb_extract_path_text(
                    cast(
                        # 额外信息取
                        SaleOrderItem.extra,
                        JSONB,
                    ),
                    "goodsDiscounts"
                ).label("goods_discounts"),

                # 购买数量
                SaleOrderItem.purchase_quantity,

                # goods_spec
                SaleOrderItem.goods_spec,
                # goods_package_sku_id
                SaleOrderItem.goods_package_sku_id,
                # sku_code
                SaleOrderItem.sku_code,
                # goods_specification
                SaleOrderItem.goods_specification,

                # 条码
                SaleOrderItem.barcode,


                # 退款数量
                case(
                    (
                        or_(
                            return_table_data.c.refund_quantity.is_(None),
                            return_table_data.c.refund_quantity == 0,
                        ),
                        None,
                    ),
                    else_=cast(return_table_data.c.refund_quantity, String),
                ).label("return_quantity"),
                # 原小计
                func.concat(
                    "￥",
                    func.round(
                        cast(SaleOrderItem.selling_price, Numeric)
                        * cast(SaleOrderItem.purchase_quantity, Numeric),
                        2,
                    ),
                ).label("total_price_item"),
                # 限时特价
                case(
                    (
                        func.jsonb_extract_path_text(
                            cast(SaleOrderItem.extra, JSONB),
                            "limit_time_special",
                            "discount_price",
                        ).is_not(None),
                        func.concat(
                            "￥",
                            func.jsonb_extract_path_text(
                                cast(SaleOrderItem.extra, JSONB),
                                "limit_time_special",
                                "discount_price",
                            ),
                        ),
                    ),
                    else_="-",
                ).label("limit_time_special_price"),
                # 折后小计
                case(
                    (
                        # 当存在特价商品时的计算
                        func.jsonb_extract_path_text(
                            cast(SaleOrderItem.extra, JSONB),
                            "limit_time_special",
                            "discount_num",
                        ).isnot(None),
                        # 计算公式：原价 * (总数量 - 特价数量) + 特价 * 特价数量
                        func.concat(
                            "￥",
                            cast(
                                func.round(
                                    cast(SaleOrderItem.selling_price, Numeric)
                                    * (
                                        cast(SaleOrderItem.purchase_quantity, Numeric)
                                        - cast(
                                        func.coalesce(
                                            func.jsonb_extract_path_text(
                                                cast(SaleOrderItem.extra, JSONB),
                                                "limit_time_special",
                                                "discount_num",
                                            ),
                                            "0",
                                        ),
                                        Numeric,
                                    )
                                    )
                                    + cast(
                                        func.coalesce(
                                            func.jsonb_extract_path_text(
                                                cast(SaleOrderItem.extra, JSONB),
                                                "limit_time_special",
                                                "discount_price",
                                            ),
                                            func.coalesce(
                                                SaleOrderItem.discount_price_in_shopcar,
                                                SaleOrderItem.selling_price,
                                            ),
                                        ),
                                        Numeric,
                                    )
                                    * cast(
                                        func.coalesce(
                                            func.jsonb_extract_path_text(
                                                cast(SaleOrderItem.extra, JSONB),
                                                "limit_time_special",
                                                "discount_num",
                                            ),
                                            "0",
                                        ),
                                        Numeric,
                                    ),
                                    2,
                                ),
                                String,
                            ),
                        ),
                    ),
                    # 当不存在特价商品时的计算
                    else_=func.concat(
                        "￥",
                        cast(
                            func.round(
                                cast(
                                    func.coalesce(
                                        SaleOrderItem.discount_price_in_shopcar,
                                        SaleOrderItem.selling_price,
                                    ),
                                    Numeric,
                                )
                                * cast(SaleOrderItem.purchase_quantity, Numeric),
                                2,
                            ),
                            String,
                        ),
                    ),
                ).label("after_discount_subtotal"),
                # 前端的折后小计
                func.jsonb_extract_path_text(
                    cast(SaleOrderItem.extra, JSONB),
                    "afterDiscountPrice",
                ).label("after_discount_price_from_front"),

                # 含特价商品数量
                case(
                    (
                        or_(
                            func.jsonb_extract_path_text(
                                cast(SaleOrderItem.extra, JSONB),
                                "limit_time_special",
                                "discount_num",
                            ).is_(None),
                            func.jsonb_extract_path_text(
                                cast(SaleOrderItem.extra, JSONB),
                                "limit_time_special",
                                "discount_num",
                            )
                            == "0",
                            func.jsonb_extract_path_text(
                                cast(SaleOrderItem.extra, JSONB),
                                "limit_time_special",
                                "discount_num",
                            )
                            == SaleOrderItem.purchase_quantity,
                        ),
                        None,
                    ),
                    else_=func.jsonb_extract_path_text(
                        cast(SaleOrderItem.extra, JSONB),
                        "limit_time_special",
                        "discount_num",
                    ),
                ).label("limit_time_special_quantity"),
                # 销售金额
                func.concat("￥", SaleOrderItem.actual_receive_price).label(
                    "actual_receive_price"
                ),
                # 退款状态
                case(
                    (
                        and_(
                            return_table_data.c.refund_quantity.isnot(None),
                            cast(SaleOrderItem.purchase_quantity, Numeric)
                            == return_table_data.c.refund_quantity,
                        ),
                        "已退款",
                    ),
                    (return_table_data.c.refund_quantity.isnot(None), "部分退款"),
                    else_="-",
                ).label("refund_status"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderItem,
                SaleOrder.record_id == SaleOrderItem.order_id,
                # isouter=True,
            )
            .join(
                return_table_data,
                return_table_data.c.id == SaleOrderItem.id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
        )
        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]

        for record in records:
            # 获取商品折扣信息
            goods_discounts = record.get("goods_discounts")
            if goods_discounts:
                goods_discounts = json.loads(goods_discounts)
                record["goods_discounts"] = goods_discounts

            after_discount_price_from_front = record.get("after_discount_price_from_front")
            if after_discount_price_from_front is not None:
                record["after_discount_subtotal"] = after_discount_price_from_front

        return records

    async def do_get_goods_price_statistic(self, record_id: int, company_id: int):
        """
        获取商品价格统计
        """
        summary_query = (
            select(
                # 总购买数量
                func.sum(cast(SaleOrderItem.purchase_quantity, Numeric)).label(
                    "total_quantity"
                ),
                # 原小计
                func.min(SaleOrder.total_origin_price).label("total_price"),
                # func.round(
                #     func.sum(
                #         cast(SaleOrderItem.selling_price, Numeric)
                #         * cast(SaleOrderItem.purchase_quantity, Numeric)
                #     ),
                #     2,
                # ).label("total_price"),
                # 总折后小计
                func.round(
                    func.sum(
                        case(
                            (
                                func.jsonb_extract_path_text(
                                    cast(SaleOrderItem.extra, JSONB),
                                    "limit_time_special",
                                    "discount_num",
                                ).isnot(None),
                                # 特价商品的计算
                                cast(SaleOrderItem.selling_price, Numeric)
                                * (
                                    cast(SaleOrderItem.purchase_quantity, Numeric)
                                    - cast(
                                    func.coalesce(
                                        func.jsonb_extract_path_text(
                                            cast(SaleOrderItem.extra, JSONB),
                                            "limit_time_special",
                                            "discount_num",
                                        ),
                                        "0",
                                    ),
                                    Numeric,
                                )
                                )
                                + cast(
                                    func.coalesce(
                                        func.jsonb_extract_path_text(
                                            cast(SaleOrderItem.extra, JSONB),
                                            "limit_time_special",
                                            "discount_price",
                                        ),
                                        func.coalesce(
                                            SaleOrderItem.discount_price_in_shopcar,
                                            SaleOrderItem.selling_price,
                                        ),
                                    ),
                                    Numeric,
                                )
                                * cast(
                                    func.coalesce(
                                        func.jsonb_extract_path_text(
                                            cast(SaleOrderItem.extra, JSONB),
                                            "limit_time_special",
                                            "discount_num",
                                        ),
                                        "0",
                                    ),
                                    Numeric,
                                ),
                            ),
                            # 非特价商品的计算
                            else_=cast(
                                func.coalesce(
                                    SaleOrderItem.discount_price_in_shopcar,
                                    SaleOrderItem.selling_price,
                                ),
                                Numeric,
                            )
                                  * cast(SaleOrderItem.purchase_quantity, Numeric),
                        )
                    ),
                    2,
                ).label("total_after_discount"),
                # 前端的折后小计相加
                func.round(
                    func.sum(
                        cast(
                            func.jsonb_extract_path_text(
                                cast(SaleOrderItem.extra, JSONB),
                                "afterDiscountPrice",
                            ),
                            Numeric,
                        )
                    ),
                    2,
                ).label("after_discount_price_from_front"),

                # 总销售金额
                cast(
                    func.round(
                        func.sum(cast(SaleOrderItem.actual_receive_price, Numeric)), 2
                    ),
                    String,
                ).label("total_actual_receive"),
            )
            .select_from(SaleOrder)
            .join(SaleOrderItem, SaleOrder.record_id == SaleOrderItem.order_id)
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
        )
        # 执行查询
        summary_result = await self.db_session.execute(summary_query)
        summary = dict(summary_result.fetchone()._mapping)

        after_discount_price_from_front = summary.get("after_discount_price_from_front")
        if after_discount_price_from_front is not None:
            summary["total_after_discount"] = after_discount_price_from_front

        return summary

    async def do_get_order_base_price_info(self, record_id: int, company_id: int):
        """
        获取订单折扣详情价格
        """
        state_case = self.sale_order_state_trans()
        price_base_info = (
            select(
                SaleOrder.id,
                state_case.label("state_name"),
                func.round(
                    func.sum(
                        case(
                            (
                                or_(
                                    func.jsonb_extract_path_text(
                                        cast(SaleOrderItem.extra, JSONB),
                                        "limit_time_special",
                                        "discount_num",
                                    ).is_(None),
                                    func.jsonb_extract_path_text(
                                        cast(SaleOrderItem.extra, JSONB),
                                        "limit_time_special",
                                        "discount_num",
                                    )
                                    == "0",
                                ),
                                cast(SaleOrderItem.shop_price, Numeric),
                            ),
                            else_=cast(
                                SaleOrderItem.origin_total_price_in_shopcaritem, Numeric
                            ),
                        )
                    ),
                    2,
                ).label("subtotal"),
                cast(SaleOrder.change_money, Numeric).label("change_money"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderItem,
                SaleOrder.record_id == SaleOrderItem.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
            .group_by(SaleOrder.id, state_case)
        ).cte("orde_detail_price_base_info")
        refund_price = (
            select(
                SaleOrder.id,
                func.round(
                    func.sum(
                        func.coalesce(
                            cast(SaleOrderRefundPayment.refund_payment_amount, Numeric),
                            0,
                        )
                    ),
                    2,
                ).label("actually_refund_amount_all"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderReturn,
                SaleOrder.record_id == SaleOrderReturn.order_id,
                isouter=True,
            )
            .join(
                SaleOrderRefundPayment,
                SaleOrderReturn.record_id == SaleOrderRefundPayment.order_refund_id,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderRefundPayment.is_refund_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_detail_refund_price")
        pay_success_price = (
            select(
                SaleOrder.id,
                cast(
                    func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), String
                ).label("payment_amount"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(True),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_detail_pay_success_price")
        pay_fail_info = (
            select(
                SaleOrder.id,
                func.round(
                    func.sum(cast(SaleOrderPayment.payment_amount, Numeric)), 2
                ).label("fail_pay_amount"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderPayment.is_pay_success.is_(False),
                )
            )
            .group_by(SaleOrder.id)
        ).cte("orde_detail_pay_fial_info")
        main_query = (
            select(
                price_base_info.c.subtotal,
                price_base_info.c.change_money,
                cast(refund_price.c.actually_refund_amount_all, String).label(
                    "actually_refund_amount_all"
                ),
                pay_success_price.c.payment_amount,
            )
            .select_from(price_base_info)
            .join(
                pay_fail_info, price_base_info.c.id == pay_fail_info.c.id, isouter=True
            )
            .join(
                pay_success_price,
                price_base_info.c.id == pay_success_price.c.id,
                isouter=True,
            )
            .join(refund_price, price_base_info.c.id == refund_price.c.id, isouter=True)
        )
        # 执行查询
        result = await self.db_session.execute(main_query)
        record = result.fetchone()
        return dict(record._mapping) if record else None

    async def do_get_order_discount_price_info(self, record_id: int, company_id: int):
        system_discount = (
            select(
                func.cast("系统抹零", String).label("name"),
                func.round(
                    func.sum(cast(SaleOrderDiscount.discount_amount, Numeric)), 2
                ).label("amount"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderDiscount,
                SaleOrder.record_id == SaleOrderDiscount.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderDiscount.discount_source == 1,
                )
            )
            .group_by(SaleOrderDiscount.discount_source)
        ).cte("system_discount_data")
        other_discount = (
            select(
                case(
                    (SaleOrderDiscount.discount_name == "单品改价", "单品让价"),
                    else_=SaleOrderDiscount.discount_name,
                ).label("name"),
                func.round(
                    func.sum(cast(SaleOrderDiscount.discount_amount, Numeric)), 2
                ).label("amount"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderDiscount,
                SaleOrder.record_id == SaleOrderDiscount.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                    SaleOrderDiscount.discount_source != 1,
                )
            )
            .group_by(SaleOrderDiscount.discount_name)
        ).cte("other_discount_data")
        main_query = union_all(select(system_discount), select(other_discount))
        # 执行查询
        result = await self.db_session.execute(main_query)
        records = result.fetchall()
        return [dict(record._mapping) for record in records]

    async def do_get_pay_info(self, record_id: int, company_id: int):
        """
        获取订单支付信息
        """
        # 获取销售订单状态转换
        state_case = self.sale_order_state_trans()
        main_query = (
            select(
                SaleOrderPayment.payment_method_name,
                case(
                    # 没有支付成功
                    (SaleOrderPayment.is_pay_success.is_(False), "0.00"),
                    else_=SaleOrderPayment.payment_amount,
                ).label("payment_amount"),
                state_case.label("state_name"),
            )
            .select_from(SaleOrder)
            .join(
                SaleOrderPayment,
                SaleOrder.record_id == SaleOrderPayment.order_id,
                isouter=True,
            )
            .where(
                and_(
                    SaleOrder.disabled.is_(False),
                    SaleOrder.company_id == company_id,
                    SaleOrder.id == record_id,
                )
            )
        )
        result = await self.db_session.execute(main_query)
        records = [dict(row._mapping) for row in result.fetchall()]
        return records
