import java.util.Random;
import java.util.function.Supplier;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GG1CircuitBreakerExample {
    
    static final double LAMBDA = 40.0;
    static final int NUM_MODULES = 8;
    static final double[] MU = new double[NUM_MODULES];
    
    static final double CV_A = 0.5;
    static final double MEAN_IA = 1.0 / LAMBDA;
    static final double STD_IA = CV_A * MEAN_IA;
    static final double CV_S = 2.0;
    static final Random rng = new Random();
    
    static final AtomicInteger[] served = new AtomicInteger[NUM_MODULES];
    static final AtomicInteger[] blocked = new AtomicInteger[NUM_MODULES];
    static final AtomicInteger[] failed = new AtomicInteger[NUM_MODULES];
    static final double[][] serviceTimes = new double[NUM_MODULES][];
    static int totalRequests = 0;
    
    static final double SLA_RESPONSE_TIME_MS = 200.0;
    static final double SLA_AVAILABILITY = 99.5;
    static final int SLA_QUEUE_CAPACITY = 30;
    
    static int slaviolations = 0;
    static int totalSlowCalls = 0;
    
    static {
        for (int i = 0; i < NUM_MODULES; i++) {
            served[i] = new AtomicInteger(0);
            blocked[i] = new AtomicInteger(0);
            failed[i] = new AtomicInteger(0);
            MU[i] = 60.0 - i * 2.0;
            serviceTimes[i] = new double[10000];
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║       G/G/1 TANDEM SYSTEM with SLA TRACKING                ║");
        System.out.println("║       " + NUM_MODULES + " MODULES IN SERIES                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");
        
        CircuitBreaker[] cbs = new CircuitBreaker[NUM_MODULES];
        for (int i = 0; i < NUM_MODULES; i++) {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(10)
                    .failureRateThreshold(50)
                    .slowCallRateThreshold(80)
                    .slowCallDurationThreshold(Duration.ofMillis((long)SLA_RESPONSE_TIME_MS))
                    .waitDurationInOpenState(Duration.ofSeconds(3))
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .build();
            
            CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
            cbs[i] = registry.circuitBreaker("module-" + (i+1));
        }
        
        System.out.println("📊 SYSTEM CONFIGURATION");
        System.out.println("   Modules: " + NUM_MODULES);
        System.out.println("   Arrival rate (λ): " + LAMBDA + " req/s");
        System.out.println("   SLA Response Time: < " + SLA_RESPONSE_TIME_MS + "ms (ABSOLUTE)");
        System.out.println("   SLA Availability: > " + SLA_AVAILABILITY + "%");
        System.out.println("   SLA Queue Capacity: " + SLA_QUEUE_CAPACITY + " jobs\n");
        
        printKingman();
        
        System.out.println("\n--- Running for 20 seconds ---\n");
        
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String csvFile = "tandem_results_" + timestamp + ".csv";
        PrintWriter csvWriter = new PrintWriter(new FileWriter(csvFile));
        csvWriter.println("Module,Requests,Served,Blocked,Failed,SuccessRate,AvgServiceTime,MaxServiceTime,CircuitState");
        
        long startTime = System.currentTimeMillis();
        long duration = 20000;
        int requestCount = 0;
        
        while (System.currentTimeMillis() - startTime < duration) {
            
            long interArrivalMs = (long)(normalInterArrival() * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));
            requestCount++;
            totalRequests++;
            int reqId = requestCount;
            
            boolean success = true;
            long requestStartTime = System.currentTimeMillis();
            
            for (int i = 0; i < NUM_MODULES; i++) {
                final int moduleIdx = i;
                final int id = reqId;
                
                Supplier<String> call = CircuitBreaker.decorateSupplier(cbs[i], () -> {
                    try {
                        long serviceMs = lognormalServiceMs(MU[moduleIdx]);
                        serviceTimes[moduleIdx][served[moduleIdx].get()] = serviceMs;
                        Thread.sleep(serviceMs);
                        
                        if (serviceMs > SLA_RESPONSE_TIME_MS) {
                            slaviolations++;
                            totalSlowCalls++;
                        }
                        
                        served[moduleIdx].incrementAndGet();
                        return "Module " + (moduleIdx+1) + " done in " + serviceMs + "ms";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });
                
                try {
                    String result = call.get();
                    if (i == 0) {
                        System.out.printf("   Req %4d: %s%n", id, result);
                    }
                } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                    blocked[moduleIdx].incrementAndGet();
                    success = false;
                    if (i == 0) {
                        System.out.printf("   Req %4d: 🚫 BLOCKED at Module %d%n", id, moduleIdx+1);
                    }
                    break;
                } catch (Exception e) {
                    failed[moduleIdx].incrementAndGet();
                    success = false;
                    if (i == 0) {
                        System.out.printf("   Req %4d: ❌ FAILED at Module %d%n", id, moduleIdx+1);
                    }
                    break;
                }
            }
            
            long totalTime = System.currentTimeMillis() - requestStartTime;
            if (totalTime > SLA_RESPONSE_TIME_MS * NUM_MODULES) {
                slaviolations++;
            }
        }
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                    FINAL STATS                             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");
        
        System.out.println("   📊 OVERALL SYSTEM METRICS");
        System.out.println("   ─────────────────────────");
        System.out.printf("   Total requests:        %d%n", requestCount);
        System.out.printf("   Success rate:          %.2f%%%n", 
            (requestCount - totalSlowCalls) * 100.0 / requestCount);
        System.out.printf("   SLA Violations:        %d%n", slaviolations);
        System.out.printf("   Slow calls:            %d%n", totalSlowCalls);
        
        System.out.println("\n   📊 PER MODULE STATS");
        System.out.println("   ────────────────────────────────────────────────────────────");
        System.out.printf("   %-8s %-10s %-10s %-10s %-10s %-12s%n", 
            "Module", "Served", "Blocked", "Failed", "Success%", "CB State");
        System.out.println("   ────────────────────────────────────────────────────────────");
        
        for (int i = 0; i < NUM_MODULES; i++) {
            int total = served[i].get() + blocked[i].get() + failed[i].get();
            double successRate = total > 0 ? (served[i].get() * 100.0 / total) : 0;
            
            double avgService = 0;
            double maxService = 0;
            int count = served[i].get();
            for (int j = 0; j < count && j < serviceTimes[i].length; j++) {
                avgService += serviceTimes[i][j];
                if (serviceTimes[i][j] > maxService) maxService = serviceTimes[i][j];
            }
            avgService = count > 0 ? avgService / count : 0;
            
            System.out.printf("   %-8s %-10d %-10d %-10d %-10.1f %-12s%n", 
                "Mod " + (i+1),
                served[i].get(),
                blocked[i].get(),
                failed[i].get(),
                successRate,
                cbs[i].getState());
            
            csvWriter.printf("%d,%d,%d,%d,%d,%.2f,%.2f,%.2f,%s%n",
                i+1,
                served[i].get() + blocked[i].get() + failed[i].get(),
                served[i].get(),
                blocked[i].get(),
                failed[i].get(),
                successRate,
                avgService,
                maxService,
                cbs[i].getState());
        }
        
        System.out.println("   ────────────────────────────────────────────────────────────");
        
        System.out.println("\n   📊 SLA SUMMARY");
        System.out.println("   ────────────────────────────────────────────────────────────");
        System.out.printf("   SLA Response Time:   < %.0fms%n", SLA_RESPONSE_TIME_MS);
        System.out.printf("   SLA Availability:    > %.1f%%%n", SLA_AVAILABILITY);
        System.out.printf("   SLA Queue Capacity:  %d jobs%n", SLA_QUEUE_CAPACITY);
        System.out.printf("   SLA Violations:      %d%n", slaviolations);
        
        double finalAvailability = (requestCount - totalSlowCalls) * 100.0 / requestCount;
        System.out.printf("   Actual Availability: %.2f%%%n", finalAvailability);
        
        if (finalAvailability >= SLA_AVAILABILITY) {
            System.out.println("   ✅ SLA MET: Availability target achieved!");
        } else {
            System.out.println("   ⚠️ SLA NOT MET: Availability below target!");
        }
        
        System.out.println("\n   📊 CSV Export");
        System.out.println("   ────────────────────────────────────────────────────────────");
        System.out.println("   ✅ Results exported to: " + csvFile);
        
        csvWriter.close();
        
        System.out.println("\n✅ Tandem simulation complete!");
        System.out.println("📌 HALF_OPEN Key Points:");
        System.out.println("   - After 3 seconds in OPEN state, circuit moves to HALF_OPEN");
        System.out.println("   - In HALF_OPEN, only 3 test calls are allowed through");
        System.out.println("   - If ALL 3 succeed → CLOSED (full recovery)");
        System.out.println("   - If ANY fail → OPEN again (still broken)");
    }
    
    static double normalInterArrival() {
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        return Math.max(0.001, MEAN_IA + STD_IA * z);
    }
    
    static long lognormalServiceMs(double mu) {
        double meanS = 1.0 / mu;
        double sigmaLn = Math.sqrt(Math.log(1 + CV_S * CV_S));
        double muLn = Math.log(meanS) - (sigmaLn * sigmaLn) / 2.0;
        
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        double seconds = Math.exp(muLn + sigmaLn * z);
        return Math.max(1, (long)(seconds * 1000));
    }
    
    static void printKingman() {
        System.out.println("📊 KINGMAN APPROXIMATION (Theoretical Values)");
        System.out.println("   ────────────────────────────────────────────────────────────");
        for (int i = 0; i < NUM_MODULES; i++) {
            double rho = LAMBDA / MU[i];
            double ca2 = CV_A * CV_A;
            double cs2 = CV_S * CV_S;
            double wqMm1 = rho / (MU[i] * (1 - rho));
            double kingmanWq = wqMm1 * ((ca2 + cs2) / 2.0);
            double lq = LAMBDA * kingmanWq;
            
            System.out.printf("   Module %d: ρ=%.3f, Lq=%.3f, Wq=%.1fms%n", 
                (i+1), rho, lq, kingmanWq * 1000);
        }
        System.out.println("   ────────────────────────────────────────────────────────────\n");
    }
}

// #This is the command to run the code:  java -cp ".:resilience4j-circuitbreaker-2.2.0.jar:resilience4j-core-2.2.0.jar:vavr-0.10.4.jar:slf4j-api-2.0.9.jar:slf4j-simple-2.0.9.jar" GG1TandemFull