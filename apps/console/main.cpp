#include <QApplication>

#include "mainwindow.h"

int main(int argc, char* argv[])
{
    // QApplication 管理 GUI 事件循环。
    // 生命周期覆盖窗口及其子控件。
    QApplication app(argc, argv);

    QApplication::setApplicationName(
        QStringLiteral("EdgeRelay Console")
        );

    MainWindow window;
    window.show();

    return app.exec();
}