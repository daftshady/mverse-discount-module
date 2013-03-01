import datetime
from django.contrib.auth.models import User
from accounts.models import Shopper, Want, Item, APNSreceiver
from commerce.models import ItemDiscount, Stock, Contract
from commerce.configs_enum import *
from shortcuts.cache import Cache, discount_key_prefix
from shortcuts.apns import send_discount_notice
from shortcuts.message_form import WANT_COUNT_DISCOUNT_PUSH_MSG, WAKE_DISCOUNT_PUSH_MSG

class WrongDiscountTypeException(Exception):
    """Exception that deals with wrong discount type input."""
    pass

"""Discount process consists of few steps.
# Discount register
1. Make discount to `ItemDiscount`. This discount must have some pre-defined discount type. 
Discount condition may vary according to discount type. 
Therefore, we save condition info into json field instead of making column to every condition.

# Actual discount execution
2. Make private stock and pass stock info to ItemDiscount so that stock column of each discount row can be filled.
3. Make private shopper(actually, discount target). Every private stock should have private shoppers in StockShopper model.

# Condition example
want_count_discount {"want_count":50,"amount_limit":100,"discount_rate":80, "span":7}
wake_discount {"shopper_id":8874,"amount_limit":1,"discount_rate":40}
"""


class DiscountService(object):
    """Manage many possible discount actions
    By calling 'DiscountService(`some_discount_type`).execute(`some_args`)', we can execute various discount functions.
        Necessary fields
        @_executor : real execute function
        @_type : various discount type
    """
    _executor = None
    _type = None

    def __init__(self, discount_type):
        def discount_getter(type):
            return {
                DISCOUNT_TYPE_WANT_COUNT_PER_ITEM : WantCountDiscount(),
                DISCOUNT_TYPE_WAKE : WakeDiscount()
            }.get(type, None)

        _discount = discount_getter(discount_type)
        self._executor = _discount.execute if hasattr(_discount, 'execute') else None
        
        if self._executor is None:
            raise WrongDiscountTypeException("Wrong discount type!")

    def execute(self, *args):
        self._executor(*args)

    def _execute(self):
        raise NotImplementedError("Should implement execute!")

    def _get_discount(self):
        pass
 
    def _do_discount(self):
        pass

    def _get_discount_target(self):
        pass

    def _is_discountable(self):
        pass

    def _send_apns(self, shopper, item, msg):
        apns_receiver = APNSreceiver.objects. \
                    filter(shopper=shopper).order_by("-updated_at")
        if apns_receiver:
            send_discount_notice(apns_receiver[0].device_token, msg, item)
   
    
class WantCountDiscount(DiscountService):
    """Discount triggered by want count per item
    Because this discount action is called every `want_increment` action, trigger point(want count per item) should be cached.
        @_cache : Each discount is distinguished by cache key. 
        Cache key consists of `discount_key_prefix + item_id` and cache value is `discount object`. 
        Cache of the some discount should be flushed after that discount is executed.
    """

    def __init__(self):
        self._type = DISCOUNT_TYPE_WANT_COUNT_PER_ITEM
        self._cache = Cache(discount_key_prefix.DISCOUNT_TYPE_WANT_COUNT_PER_ITEM)
        self._push_msg = WANT_COUNT_DISCOUNT_PUSH_MSG
        # Is there any good way to express `no available discount` instead of `-1`?
        self._NO_AVAIL_DISCOUNT = -1

    def execute(self, want):
        self._execute(want)

    def _execute(self, want):
        item = want.item
        if (self._is_discountable(item)):
            discount = self._get_discount(item, self._type)
            stocks = self._do_discount(item, discount)
            self._cache.remove(item.id)

    def _get_discount_target(self, item):
        return Want.objects.fetch_wanted_shoppers_by_item(item)

    def _get_discount(self, item, type):
        """If there is many unexcuted discounts with same item, discount with minimum want count should be executed first. So this function returns discount with minimum want count."""
        discounts = ItemDiscount.objects.fetch_available_discounts(item, type)
        if not discounts:
            return discounts

        min_want_counts = min(map(lambda x : x.want_count, discounts))
        for discount in discounts:
            if discount.want_count == min_want_counts:
                return discount
 
    def _do_discount(self, item, discount):
        discount.set_date_info()
        shoppers = self._get_discount_target(item)
        stocks = Stock.objects.discount_item(item, discount, shoppers)
        discount.set_executed(stocks)

        # Send push here
        msg = self._push_msg % (item.title, discount.discount_rate)
        for shopper in shoppers:
            self._send_apns(shopper, item, msg)

        return stocks

    def _is_discountable(self, item):
        type = self._type
        cache = self._cache
        discount = None
        if cache.has_key(item.id):
            discount = cache.get(item.id)
            if discount == self._NO_AVAIL_DISCOUNT:
                return False
        
        if discount is None:
            discount = self._get_discount(item, type)
            cache.set(item.id, discount)
            
        if not discount:
            cache.set(item.id, self._NO_AVAIL_DISCOUNT)
            return False

        return discount.want_count <= item.wanted_count


def enum(**enums):
    return type('Enum', (), enums)
WAKE_PARAM = enum(CREATE=0, EXECUTE=1)
FETCH_POLICY = enum(SIMPLE=0, RECURSIVE=1)

class WakeDiscount(DiscountService):
    """Discount that wakes up passive shoppers.
    Because this discount is triggered by crone job, we don't actually execute discount in this class.
    Actual execution is done by `wake_discount_execute` function(crone job).
        @_DEFAULT_SHOPPER_RANGE : number of days that shopper has not logined. 
        if default_shopper_range is 5, then wake target is 'shoppers that have not logined for 5 days lately'.
        @_DEFAULT_SHOPPER_FILTER_RANGE : if some shopper has not logined after he has received push message(waked)
        that shopper should be filtered in next wake discount.(because frequent push makes shopper annoying)
        if default_shopper_filter_range is 5, then shoppers who have received push in recent 5 days will be
        filtered in next wake discount.
        @_DEFAULT_DISCOUNT_SPAN : discount span.(end_date - start_date)
        @_DEFAULT_DISCOUNT_RATE : discount rate.
        @_DEFAULT_AMOUNT_LIMIT : product amount limit in one discount.
        @_DEFAULT_START_HOUR : in executor, execution is triggered by start_date. that start_date is defined by default_start_hour of discount_creation_day. If executor catches that current_time passes by default_start_hour,
        discount execution is done by creating private stocks and private shoppers.
        @_DEFAULT_FETCH_POLICY : When fetching item, basically if a shopper wanted at least one item, that item will be discounted. If a shopper don't wanted any item, sellable item is randomly fetched.
        By the way, randomly fetched item may be undiscountable because of contract with brand.
        In this case,
            FETCH_POLICY.SIMPLE : Simply ignore that discount(don't create discount in that case), because ignored shopper will benefit tomorrow wake discount.
            FETCH_POLICY.RECURSIVE : Recursively find discountable item.
            # CAUTION : In special case, if every sellable item is undiscountable, this policy will make deadlock.(because of infinitely finding discountable item)
            # TODO : Make upper bound of searching to avoid deadlock.
        @_DEFAULT_DISTRIBUTION_FACTOR : Target shoppers will be devided with this factor to avoid leaning of discount.

    """
    def __init__(self):
        self._type = DISCOUNT_TYPE_WAKE
        self._push_msg = WAKE_DISCOUNT_PUSH_MSG
        self._DEFAULT_SHOPPER_RANGE = 150 
        self._DEFAULT_SHOPPER_FILTER_RANGE = 5
        self._DEFAULT_DISCOUNT_SPAN = 7
        self._DEFAULT_DISCOUNT_RATE = 40
        self._DEFAULT_AMOUNT_LIMIT = 10
        self._DEFAULT_START_HOUR = 15
        self._DEFAULT_DISTRIBUTION_FACTOR = 10
        self._DEFAULT_FETCH_POLICY = FETCH_POLICY.SIMPLE

    def execute(self, wake_param):
        self._execute(wake_param)

    def _execute(self, wake_param):
        if wake_param == WAKE_PARAM.CREATE:
            self._create_wake_discount()
        elif wake_param == WAKE_PARAM.EXECUTE:
            self._wake_discount_execute()
    
    def _create_wake_discount(self):
        map(self._create_discount, self._get_discount_target())

    def _get_start_date(self):
        now = datetime.datetime.now()
        return datetime.datetime.now().replace(hour=self._DEFAULT_START_HOUR, minute=0, second=0)
    
    def _get_end_date(self, start_date):
        return start_date + datetime.timedelta(days=self._DEFAULT_DISCOUNT_SPAN)

    def _get_past_date(self, delta):
        return datetime.datetime.now() + datetime.timedelta(days=-delta)

    def _get_discount_target(self):
        target_date = self._get_past_date(self._DEFAULT_SHOPPER_RANGE)
        shoppers = Shopper.objects. \
                filter(user__in=User.objects. \
                filter(last_login__lte=self._get_past_date(self._DEFAULT_SHOPPER_RANGE)))
        return self._filter_shoppers(shoppers)

    def _filter_shoppers(self, shoppers):
        recent_discounts = ItemDiscount.objects. \
                    filter(start_date__gte=self._get_past_date(self._DEFAULT_SHOPPER_FILTER_RANGE),
                    is_executed = True, discount_type = self._type)
        try:
            shopper_ids = reduce(lambda x,y : x+y, map(lambda x : x.shoppers, recent_discounts))
        except TypeError:
            shopper_ids = list()
        recent_discounted_shoppers = Shopper.objects.filter(id__in=shopper_ids)
        # To avoid leaning of discount, distribute target shopper day by day. 
        target = list(set(shoppers)-set(recent_discounted_shoppers))
        distribution = len(target) / self._DEFAULT_DISTRIBUTION_FACTOR
        return target[:distribution]

    def _create_discount(self, shopper):
        """Because wake discount is special case discount excuted by scheduler, 
        We don't create discount in advance."""
        
        item = self._fetch_item(shopper)
        discount_rate = self._get_feasible_discount_rate(item)
        
        if not self._is_discountable(discount_rate):
            if self._DEFAULT_FETCH_POLICY == FETCH_POLICY.SIMPLE:
                # We ignore that shopper(don't create discount),
                # if max discount rate of randomly fetched item is 0.
                return
            elif self._DEFAULT_FETCH_POLICY == FETCH_POLICY.RECURSIVE:
                # We recursively find discountable item.
                while not self._is_discountable(discount_rate):
                    item = self._fetch_item(shopper)
                    discount_rate = self._get_feasible_discount_rate(item)

        condition = dict(amount_limit=self._DEFAULT_AMOUNT_LIMIT,
                         discount_rate=discount_rate,
                         shopper_ids=[shopper.id])
        start_date = self._get_start_date()
        end_date = self._get_end_date(start_date)
        return ItemDiscount.objects.create_discount(item, self._type, condition, \
                                                start_date=start_date, end_date=end_date)
    
    def _is_inactive_shopper(self, shopper):
        return shopper.wanted_count == 0

    def _is_discountable(self, discount_rate):
        return discount_rate != 0

    def _get_feasible_discount_rate(self, item):
        return Contract.objects.get_feasible_discount_rate(item, self._DEFAULT_DISCOUNT_RATE)

    def _fetch_item(self, shopper):
        return self._fetch_random_item() if self._is_inactive_shopper(shopper) \
                    else self._fetch_recently_wanted_item(shopper)

    def _fetch_random_item(self):
        return Item.objects.fetch_random_item()

    def _fetch_recently_wanted_item(self, shopper):
        item = Want.objects.fetch_recently_wanted_item(shopper)
        return item if item is not None else self._fetch_random_item()
            
    def _wake_discount_execute(self):
        discounts = ItemDiscount.objects. \
            filter(is_executed=False, start_date__lte=datetime.datetime.now(), \
            discount_type=DISCOUNT_TYPE_WAKE)
        
        map(lambda x : self._do_discount(x, x.item, x.shoppers), discounts)
    
    def _do_discount(self, discount, item, shopper_ids):
        shoppers = Shopper.objects.filter(id__in=shopper_ids)
        stocks = Stock.objects.discount_item(item, discount, map(lambda x : x, shoppers))
        discount.set_executed(stocks)
        # Send push here
        msg = self._push_msg % (item.title, discount.discount_rate)
        for shopper in shoppers:
            self._send_apns(shopper, item, msg)

