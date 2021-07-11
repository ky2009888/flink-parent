#!/bin/bash
read -t 20 -p "请输入您要关闭的yarn cluster模式启动的程序ID:" appId
echo -e "\n"
echo "您输入的应用程序ID为:$appId"
/hadoop/bin/yarn application -kill $appId
if [ $? -ne 0 ]; then
    echo "关闭失败"
else
    echo "关闭$appId应用程序成功"
fi

