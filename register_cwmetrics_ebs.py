import re
import json
import boto3
import logging

"""
CloudWatch ダッシュボードにボリューム関連のメトリクスを追加する

ボリューム作成をトリガーとして起動させる用のスクリプト
"""


MAX_METRICS = 100
METRICS_VOLREAD = "VolumeReadBytes"
METRICS_VOLWRITE = "VolumeWriteBytes"
METRICS_VOLREADOPS = "VolumeReadOps"
METRICS_VOLWRITEOPS = "VolumeWriteOps"
METRICS_TEMPLATE = {
            METRICS_VOLREAD: { 
                "widget": tuple(),
                "metrics": [{"DimensionName": METRICS_VOLREAD, "VolumeId": ""}]
            },
            METRICS_VOLWRITE: {
                "widget": tuple(),
                "metrics": [{"DimensionName": METRICS_VOLWRITE, "VolumeId": ""}]
            },
            METRICS_VOLREADOPS: {
                "widget": tuple(),
                "metrics": [{"DimensionName": METRICS_VOLREADOPS, "VolumeId": ""}]
            },
            METRICS_VOLWRITEOPS: {
                "widget": tuple(),
                "metrics": [{"DimensionName": METRICS_VOLWRITEOPS, "VolumeId": ""}]
            }
}

WIDGET_WIDTH = 6
WIDGET_HEIGHT = 6
WIDGET_TEMPLATE = {
    "type": "metric",
    "x": 0,
    "y": 0,
    "width": WIDGET_WIDTH,
    "height": WIDGET_HEIGHT,
    "properties": {
        "stacked": False,
        "metrics": [],
        "title": "",
        "region": "ap-northeast-1",
        "period": 300,
    "view": "timeSeries"
    }
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def init_cwclient():
    client = boto3.client("cloudwatch")
    return client
    
def create_widget(widget_title: str,
                  metrics: list=None,
                  width: int=WIDGET_WIDTH,
                  height: int=WIDGET_HEIGHT):
    """create new widget to the specified dashboard.
    
    Args:
        widget_title (str): widget title you create
        metrics (list): metrics you want to add to widget
            [{"DimensionName": "VolumeReadBytes",
              "VolumeId": "vol-xxxx"},]
        width (int, optional): widget width. Defaults to WIDGET_WIDTH.
        height (int, optional): widget height. Defaults to WIDGET_HEIGHT.
    """
    widget = json.dumps(WIDGET_TEMPLATE)
    widget = json.loads(widget)
    widget["properties"]["title"] = widget_title
    widget["width"] = width
    widget["height"] = height
    if metrics is not None:
        for i, metric in enumerate(metrics):
            widget["properties"]["metrics"].append(["AWS/EBS",
                                    metric["DimensionName"],
                                    "VolumeId",
                                    metric["VolumeId"],
                                    {"id": "m{0}".format((i + 1)),
                                     "label": metric["VolumeId"]
                                    }])
            widget["properties"]["metrics"].append([{
                "expression": "SUM(METRICS('m{0}'))/300".format(i + 1),
                "label": metric["VolumeId"],
                "id": "e{0}".format((i + 1))}])
    return widget

def add_metrics_to_widget(widget_body: dict, metrics: list):
    """add new metrics to widget
    
    Args:
        widget_body (dict): widget body
        metrics (list): metrics you want to add to widget
            [{"DimensionName": "VolumeReadBytes",
              "VolumeId": "vol-xxxx"},]
    """
    # creates new widget when registered metrics is over limitation
    if len(widget_body["properties"]["metrics"]) > (MAX_METRICS - (len(metrics)*2)):
        match = re.match(r"(.+)(\d+)$", widget_body["properties"]["title"])
        if match:
            title = "{0}{1}".format(match.group(1), str((int(match.group(2)) + 1)))
        else:
            title = "{0} 2".format(widget_body["properties"]["title"])
        widget = create_widget(title, metrics)
        return widget

    metrics_num = len(widget_body["properties"]["metrics"])
    for i, metric in enumerate(metrics):
        widget_body["properties"]["metrics"].append(["AWS/EBS",
                                                     metric["DimensionName"],
                                                     "VolumeId",
                                                     metric["VolumeId"],
                                                     {"id": "m{0}".format((metrics_num + 1)),
                                                      "label": metric["VolumeId"]
                                                     }])
        widget_body["properties"]["metrics"].append([{
                "expression": "SUM(METRICS('m{0}'))/300".format((metrics_num + 1)),
                "label": metric["VolumeId"],
                "id": "e{0}".format((metrics_num + 1))}])
        metrics_num += 1
    return widget_body

def is_limit_regmetrics(widget: dict, metrics: list):
    """checks widget whether it register metrics
    
    Args:
        widget (dict): widget body
    """
    if len(widget["properties"]["metrics"]) > (MAX_METRICS - (len(metrics)*2)):
        return True
    else:
        return False

def lambda_handler(event, context):
    if event["detail"]["result"] == "available":
        dbname = event['params']['dboard_name']
        volid = event['resources'][0].split("/")[1]
        dbody = {"widgets": []}
        newwidget = dict()
        reged_widgets = METRICS_TEMPLATE
        client = init_cwclient()

        logger.info("Dashboard: {0}, VolumeId: {1}".format(dbname, volid))

        rexp_volread = re.compile(r"{0}".format(METRICS_VOLREAD))
        rexp_volwrite = re.compile(r"{0}".format(METRICS_VOLWRITE))
        rexp_volreadops = re.compile(r"{0}".format(METRICS_VOLREADOPS))
        rexp_volwriteops = re.compile((r"{0}".format(METRICS_VOLWRITEOPS)))
        
        dashboard = client.get_dashboard(DashboardName=dbname)
        dashboard = json.loads(dashboard["DashboardBody"])
        widgets = dashboard["widgets"]
        # set VolumeId of created volume to metrics
        for key in reged_widgets.keys():
            for metric in reged_widgets[key]["metrics"]:
                metric["VolumeId"] = volid
        # set registered widgets per metrics
        for widget in widgets:
            title = widget["properties"]["title"]
            if rexp_volread.match(title):
                reged_widgets[METRICS_VOLREAD]["widget"] += (widget,)
                continue
            if rexp_volwrite.match(title):
                reged_widgets[METRICS_VOLWRITE]["widget"] += (widget,)
                continue
            if rexp_volreadops.match(title):
                reged_widgets[METRICS_VOLREADOPS]["widget"] += (widget,)
                continue
            if rexp_volwriteops.match(title):
                reged_widgets[METRICS_VOLWRITEOPS]["widget"] += (widget,)
                continue
            dbody["widgets"].append(widget)
        # sort widgets with title key
        reged_widgets = { key: {"widget": sorted(reged_widgets[key]["widget"], key=lambda x: x["properties"]["title"]), \
                                "metrics": reged_widgets[key]["metrics"]}
                        for key in reged_widgets.keys() }
        # add metrics to widget
        for key,val in reged_widgets.items():
            widget_num = len(val["widget"])
            metrics = val["metrics"]
            lastindex = widget_num - 1
            # create a new widget if its does not exists
            if widget_num == 0:
                newwidget = create_widget(key, val["metrics"])
            # add metrics to existed widget
            elif widget_num == 1:
                if is_limit_regmetrics(val["widget"][lastindex], metrics):
                    dbody["widgets"].append(val["widget"][lastindex])
                newwidget = add_metrics_to_widget(val["widget"][lastindex], metrics)
            # add metrics to existed widget with last number if its exists multiple
            else:
                for i,widget in enumerate(val["widget"]):
                    if i >= lastindex:
                        if is_limit_regmetrics(widget, metrics):
                            dbody["widgets"].append(widget)
                        newwidget = add_metrics_to_widget(widget, metrics)
                        break
                    dbody["widgets"].append(widget)
            dbody["widgets"].append(newwidget)

        # apply updates to dashboard
        dbody = json.dumps(dbody)
        logger.info("Add folowing dashboard:\n{0}".format(dbody))
        client.put_dashboard(DashboardName=dbname,
                                DashboardBody=dbody)
        return {"result": "Success to add ebs metrics {0} to {1}".format(volid, dbname),
                "responsecode": 200}
    else:
        return {"result": "createvolume was failed", "responscode": -1}
