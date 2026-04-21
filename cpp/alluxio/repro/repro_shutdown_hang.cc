// Minimal reproducer for the S3CrtClient cache shutdown ordering bug.
//
// Context:
//   Before the fix, AlluxioClient held a process-wide cache of
//   shared_ptr<S3CrtClient> as a class-static member. That cache was
//   destroyed by the C++ static-destruction chain AFTER main() returned,
//   which happened AFTER Aws::ShutdownAPI() had already been called
//   (from the function-local static AlluxioInit in obj_open_backend).
//   S3CrtClient destructors joined their CRT thread pools with the
//   underlying aws-c-* runtime state already gone — hanging on
//   pthread_join.
//
// Observed at 6-node plugin-mode benchmark:
//   - Python ITER 1 finishes; process never exits
//   - 518 threads alive, 0 established TCP, State: S (sleeping)
//   - CPU 350% declining to 210% (not zero — threads busy-polling)
//   - 4 nodes worked; 6 nodes hung every time
//
// This reproducer isolates the bug from Python / runai / Alluxio: it
// creates N S3CrtClients pointing at distinct (non-listening) endpoints
// and arranges destruction to either reproduce the bug (--mode=static)
// or avoid it (--mode=ordered).
//
// Usage:
//   repro_shutdown_hang --n=6 --mode=static    # expect: hang, watchdog aborts with 124
//   repro_shutdown_hang --n=6 --mode=ordered   # expect: clean exit 0
//   repro_shutdown_hang --n=4 --mode=static    # maybe clean, maybe hang — the scale threshold
//
// Threat model: this reproducer does NOT hit the network. Each client
// is pointed at 127.0.0.1:<unused port>. The bug is in destructor
// ordering and reproduces without any GET ever being issued; creating
// the CRT client is enough to spawn its thread pool.

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace
{

// Process-wide static cache — mimics AlluxioClient::_shared_worker_clients
// as it was before the fix. Destructor runs during C++ static-destruction
// phase, i.e. AFTER main() returns. If Aws::ShutdownAPI was called inside
// main, the CRT teardown here runs with the SDK runtime already gone.
std::unordered_map<std::string, std::shared_ptr<Aws::S3Crt::S3CrtClient>>
    g_static_cache;
std::mutex g_static_cache_mu;

std::shared_ptr<Aws::S3Crt::S3CrtClient> make_client(int port)
{
    Aws::S3Crt::ClientConfiguration cfg;
    cfg.verifySSL = false;
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.useVirtualAddressing = false;
    cfg.endpointOverride =
        "http://127.0.0.1:" + std::to_string(port);
    return std::make_shared<Aws::S3Crt::S3CrtClient>(cfg);
}

void arm_watchdog(int seconds)
{
    std::thread([seconds]{
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        std::cerr << "[watchdog] static destruction hung >"
                  << seconds << "s — aborting with code 124\n";
        std::cerr.flush();
        _exit(124);
    }).detach();
}

int parse_eq(const std::string& a, const std::string& prefix)
{
    return std::stoi(a.substr(prefix.size()));
}

} // anon

int main(int argc, char** argv)
{
    int n_endpoints = 6;
    std::string mode = "static";
    int watchdog_s = 15;

    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        if (a.rfind("--n=", 0) == 0)
        {
            n_endpoints = parse_eq(a, "--n=");
        }
        else if (a.rfind("--mode=", 0) == 0)
        {
            mode = a.substr(std::string("--mode=").size());
        }
        else if (a.rfind("--watchdog=", 0) == 0)
        {
            watchdog_s = parse_eq(a, "--watchdog=");
        }
        else
        {
            std::cerr << "unknown arg: " << a << "\n"
                      << "usage: " << argv[0]
                      << " [--n=N] [--mode=static|ordered] [--watchdog=SECS]\n";
            return 2;
        }
    }

    std::cerr << "mode=" << mode
              << " n_endpoints=" << n_endpoints
              << " watchdog=" << watchdog_s << "s\n";

    // Arm watchdog FIRST — if any subsequent call hangs (including InitAPI),
    // the watchdog still fires and we learn where we got stuck.
    arm_watchdog(watchdog_s);

    std::cerr << "calling Aws::InitAPI\n";
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    std::cerr << "Aws::InitAPI returned\n";

    {
        std::vector<std::shared_ptr<Aws::S3Crt::S3CrtClient>> local_cache;
        // Ports 30000+i — nothing listening. CRT still constructs thread
        // pool + endpoint config; we don't need the network for the bug.
        for (int i = 0; i < n_endpoints; ++i)
        {
            int port = 30000 + i;
            auto client = make_client(port);

            if (mode == "static")
            {
                std::lock_guard<std::mutex> g(g_static_cache_mu);
                g_static_cache.emplace(
                    "http://127.0.0.1:" + std::to_string(port), client);
            }
            else if (mode == "ordered")
            {
                local_cache.push_back(client);
            }
            else
            {
                std::cerr << "unknown mode: " << mode
                          << " (expected static or ordered)\n";
                return 2;
            }
        }

        std::cerr << "all " << n_endpoints << " S3CrtClient instances constructed\n";

        // In ordered mode, local_cache's shared_ptrs drop at this
        // closing brace — client destructors run BEFORE Aws::ShutdownAPI.
        // In static mode, g_static_cache still holds them.
    }

    if (mode == "ordered")
    {
        std::cerr << "local clients destroyed; calling Aws::ShutdownAPI\n";
    }
    else
    {
        std::cerr << "static cache retains " << g_static_cache.size()
                  << " clients; calling Aws::ShutdownAPI anyway\n";
    }

    auto t0 = std::chrono::steady_clock::now();
    Aws::ShutdownAPI(options);
    auto shutdown_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::cerr << "Aws::ShutdownAPI returned after "
              << shutdown_ms << " ms\n";

    std::cerr << "main() returning; watchdog armed ("
              << watchdog_s << "s). In --mode=static, g_static_cache's "
              << "destructor runs NOW (post-ShutdownAPI) and may hang.\n";
    std::cerr.flush();

    // Return triggers C++ static destruction phase. In static mode,
    // g_static_cache's destructor drops every shared_ptr, invoking
    // S3CrtClient::~S3CrtClient which tries to join threads owned by
    // an already-torn-down aws-c-s3 runtime → hang on pthread_join.
    // Watchdog aborts after `watchdog_s` seconds with code 124.
    //
    // In ordered mode, g_static_cache is empty; static destruction is
    // a no-op; process exits with code 0 well before watchdog fires.
    return 0;
}
