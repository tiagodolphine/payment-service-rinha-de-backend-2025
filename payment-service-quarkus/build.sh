mvn clean package \
-Dquarkus.native.enabled=true \
-Dquarkus.native.container-build=true \
-Dquarkus.container-image.build=true  \
-Dquarkus.container-image.name="payment-service-quarkus" \
-Dquarkus.container-image.username=tiagodolphine
