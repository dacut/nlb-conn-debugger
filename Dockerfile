FROM scratch
COPY nlb-conn-debugger /
ENTRYPOINT ["/nlb-conn-debugger"]
