# From Paris to Vienna
## A Journey Through Geodesy and Great Circle Distances

> What is the great circle distance between Paris and Vienna?

The great circle distance between Paris, France, and Vienna, Austria, is approximately 1,036 kilometers (644 miles).

> Can you explain your calculation in more detail. Including the method and used coordinates?

Sure, I can explain the calculation in more detail. To calculate the great circle distance between two points on Earth, we use the haversine formula. The haversine formula requires the latitude and longitude coordinates of both points.

    For Paris, France:
    Latitude: 48.8566° N
    Longitude: 2.3522° E

    For Vienna, Austria:
    Latitude: 48.2082° N
    Longitude: 16.3738° E

***Step 1: Convert the latitude and longitude from degrees to radians***

    Paris:
    Latitude: 0.8529 radians
    Longitude: 0.0410 radians

    Vienna:
    Latitude: 0.8413 radians
    Longitude: 0.2857 radians

***Step 2: Calculate the differences in latitude and longitude between the two points.***

    Delta Latitude: 0.0116 radians
    Delta Longitude: 0.2447 radians

***Step 3: Apply the haversine formula***

    a = sin²(Delta Latitude/2) + cos(Latitude1) * cos(Latitude2) * sin²(Delta Longitude/2)
    a = (sin(0.0116/2))^2 + cos(0.8529) * cos(0.8413) * (sin(0.2447/2))^2
    a ≈ 0.001164

    c = 2 * atan2(sqrt(a), sqrt(1-a))
    c ≈ 0.0483

***Step 4: Calculate the great circle distance***

    Earth's radius: 6,371 kilometers (3,959 miles)

    Distance = Earth's radius * c
    Distance ≈ 6,371 * 0.0483
    Distance ≈ 1,036 kilometers (644 miles)

So, the great circle distance between Paris and Vienna is approximately 1,036 kilometers (644 miles).

>  Can you do the calculation using a WGS 84 spheroid instead of assuming a perfectly round earth?

Certainly! When using the WGS 84 spheroid, we can calculate the great circle distance between two points using the Vincenty's formulae, which provides more accurate results by considering Earth's ellipsoidal shape. The WGS 84 spheroid has a semi-major axis (a) of 6,378.137 km and a flattening (f) of 1/298.257223563.

    For Paris, France:
    Latitude (ϕ1): 48.8566° N
    Longitude (λ1): 2.3522° E

    For Vienna, Austria:
    Latitude (ϕ2): 48.2082° N
    Longitude (λ2): 16.3738° E

***Step 1: Convert the latitude and longitude from degrees to radians.***

    Paris:
    Latitude: 0.8529 radians
    Longitude: 0.0410 radians

    Vienna:
    Latitude: 0.8413 radians
    Longitude: 0.2857 radians

***Step 2: Calculate the differences in latitude and longitude between the two points.***

    Delta Latitude (Δϕ): 0.0116 radians
    Delta Longitude (Δλ): 0.2447 radians

***Step 3: Calculate the reduced latitudes (U).***

    U1 = atan((1-f) * tan(ϕ1))
    U1 ≈ atan(0.99664719 * tan(0.8529))
    U1 ≈ 0.8505 radians

    U2 = atan((1-f) * tan(ϕ2))
    U2 ≈ atan(0.99664719 * tan(0.8413))
    U2 ≈ 0.8390 radians

***Step 4: Iterate to find the angular distance (σ) using Vincenty's formulae.***

Initial approximation: L = Δλ
We iterate using the following equations until the difference between successive L values is small enough (e.g., < 10^(-12)):

    sin(σ) = sqrt((cos(U2) * sin(Δλ))^2 + (cos(U1) * sin(U2) - sin(U1) * cos(U2) * cos(Δλ))^2)
    cos(σ) = sin(U1) * sin(U2) + cos(U1) * cos(U2) * cos(Δλ)
    sin(α) = cos(U1) * cos(U2) * sin(Δλ) / sin(σ)
    cos²(α) = 1 - sin²(α)
    cos(2σm) = cos(σ) - 2 * sin(U1) *

    sin(U2) / cos²(α)
    C = f/16 * cos²(α) * (4 + f * (4 - 3 * cos²(α)))
    Δλ_new = Δλ + (1 - C) * f * sin(α) * (σ + C * sin(σ) * (cos(2σm) + C * cos(σ) * (-1 + 2 * cos²(2σm))))

After iterating, we get:
σ ≈ 0.0486 radians

***Step 5: Calculate the great circle distance (s).***

    s = b * A(σ)

    b = a * (1 - f)^0.5 = 6,356.752314245 km (semi-minor axis of WGS 84 spheroid)
    A(σ) = 1 + f/4 * (1 - f)^2 * (4 - 3 * (1 - f)^2)
    s ≈ 6,356.752314245 * 1.00067521 * 0.0486
    s ≈ 1,042.5 km

So, the great circle distance between Paris and Vienna using the WGS 84 spheroid is approximately 1,042.5 kilometers.
