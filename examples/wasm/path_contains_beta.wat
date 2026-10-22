;; ChaosProxy-L7 WebAssembly matcher, ABI v1.
;;
;; Applies the rule only when the request summary contains the bytes "beta"
;; (for example a path like /beta/checkout). See src/wasm.rs for the ABI.
;;
;; Input layout written by the proxy at the pointer returned by chaos_alloc:
;;   v1\n<METHOD>\n<path>\n[<header>: <value>\n ...]
(module
  (memory (export "memory") 1)
  ;; The needle lives at offset 0; inputs are written from offset 1024.
  (data (i32.const 0) "beta")

  (func (export "chaos_abi_version") (result i32)
    (i32.const 1))

  ;; One fixed buffer is enough: each decision runs in a fresh instance and the
  ;; proxy never sends more than 8 KiB.
  (func (export "chaos_alloc") (param $len i32) (result i32)
    (i32.const 1024))

  ;; Returns 1 if the 4 bytes "beta" appear in [ptr, ptr+len), else 0.
  (func (export "chaos_decide") (param $ptr i32) (param $len i32) (result i32)
    (local $i i32)
    (local $last i32)
    (if (i32.lt_s (local.get $len) (i32.const 4))
      (then (return (i32.const 0))))
    (local.set $last (i32.sub (i32.add (local.get $ptr) (local.get $len)) (i32.const 4)))
    (local.set $i (local.get $ptr))
    (block $done
      (loop $scan
        (br_if $done (i32.gt_s (local.get $i) (local.get $last)))
        (if (i32.eq (i32.load (local.get $i)) (i32.load (i32.const 0)))
          (then (return (i32.const 1))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $scan)))
    (i32.const 0)))
