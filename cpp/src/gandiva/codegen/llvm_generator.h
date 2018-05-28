/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_LLVMGENERATOR_H
#define GANDIVA_LLVMGENERATOR_H

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <gtest/gtest_prod.h>
#include "common/gandiva_aliases.h"
#include "codegen/dex_visitor.h"
#include "codegen/compiled_expr.h"
#include "codegen/engine.h"
#include "codegen/function_registry.h"
#include "codegen/value_validity_pair.h"
#include "codegen/llvm_types.h"
#include "codegen/lvalue.h"
#include "expr/annotator.h"
#include "expr/expression.h"

namespace gandiva {

/// Builds an LLVM module and generates code for the specified set of expressions.
class LLVMGenerator {
 public:
  LLVMGenerator();
  ~LLVMGenerator();

  /// \brief Build the code for the expression trees. Each element in the vector
  /// represents an expression tree
  void Build(const ExpressionVector &exprs);

  /// \brief Execute the built expression against the provided arguments.
  int Execute(const arrow::RecordBatch &record_batch, const arrow::ArrayVector &outputs);

 private:
  FRIEND_TEST(TestLLVMGenerator, TestAdd);
  FRIEND_TEST(TestLLVMGenerator, TestIntersectBitMaps);

  llvm::Module *module() { return engine_->module(); }
  llvm::LLVMContext &context() { return *(engine_->context()); }
  llvm::IRBuilder<> &ir_builder() { return engine_->ir_builder(); }
  LLVMTypes &types() { return types_; }

  /// Visitor to generate the code for a decomposed expression.
  class Visitor : public DexVisitor {
   public:
    Visitor(LLVMGenerator *generator,
            llvm::Function *function,
            llvm::BasicBlock *entry_block,
            llvm::BasicBlock *loop_block,
            llvm::Value *arg_addrs,
            llvm::Value *loop_var);

    void Visit(const VectorReadValidityDex &dex) override;
    void Visit(const VectorReadValueDex &dex) override;
    void Visit(const LiteralDex &dex) override;
    void Visit(const NonNullableFuncDex &dex) override;
    void Visit(const NullableNeverFuncDex &dex) override;

    LValuePtr result() { return result_; }

   private:
    llvm::IRBuilder<> &ir_builder() { return generator_->ir_builder(); }
    llvm::Module *module() { return generator_->module(); }

    // Generate the code to build the combined validity (bitwise and) from the
    // vector of validities.
    llvm::Value *BuildCombinedValidity(const DexVector &validities);

    void AddTrace(const std::string &msg, llvm::Value *value = nullptr) {
      generator_->AddTrace(msg, value);
    }

    LLVMGenerator *generator_;
    LValuePtr result_;
    llvm::BasicBlock *entry_block_;
    llvm::BasicBlock *loop_block_;
    llvm::Value *arg_addrs_;
    llvm::Value *loop_var_;
  };

  // Generate the code for one expression, with the output of the expression going to
  // 'output'.
  void Add(const ExpressionPtr expr, const FieldDescriptorPtr output);

  /// Generate code to load the vector at specified index in the 'arg_addrs' array.
  llvm::Value *LoadVectorAtIndex(llvm::Value *arg_addrs,
                                 int idx,
                                 const std::string &name);

  /// Generate code to load the vector at specified index and cast it as bitmap.
  llvm::Value *GetValidityReference(llvm::Value *arg_addrs,
                                    int idx,
                                    FieldPtr field);

  /// Generate code to load the vector at specified index and cast it as data array.
  llvm::Value *GetDataReference(llvm::Value *arg_addrs,
                                int idx,
                                FieldPtr field);

  /// Generate code for the value array of one expression.
  llvm::Function *CodeGenExprValue(DexPtr value_expr,
                                   FieldDescriptorPtr output,
                                   int suffix_idx);

  /// Generate code to get the bit value at 'position' in the bitmap.
  llvm::Value *GetPackedBitValue(llvm::Value *bitMap, llvm::Value *position);

  /// Generate code to set the bit value at 'position' in the bitmap to 'value'.
  void SetPackedBitValue(llvm::Value *bitMap, llvm::Value *position, llvm::Value *value);

  /// Generate code to make a function call (to a pre-compiled IR function) which takes
  /// 'args' and has a return type 'ret_type'.
  llvm::Value *AddFunctionCall(const std::string &full_name,
                               llvm::Type *ret_type,
                               const std::vector<llvm::Value *> &args);

  /// Compute the result bitmap for the expression.
  ///
  /// \param[in] : the compiled expression (includes the bitmap indices to be used for
  ///              computing the validity bitmap of the result).
  /// \param[in] : raw buffers from a record batch.
  /// \param[in] : number of buffers
  /// \param[in] : number of records in the batch (same as #bits in the bitmap).
  void ComputeBitMapsForExpr(CompiledExpr *compiled_expr,
                             uint8_t **buffers,
                             int num_buffers,
                             int record_count);

  /// Compute the result bitmap by doing a bitwise-and of the source bitmaps.
  static void IntersectBitMaps(uint8_t *dst_map,
                               const std::vector<uint8_t *> &src_maps,
                               int num_records);

  /// Replace the %T in the trace msg with the correct type corresponding to 'type'
  /// eg. %d for int32, %ld for int64, ..
  std::string ReplaceFormatInTrace(const std::string &msg,
                                   llvm::Value *value,
                                   std::string *print_fn);

  /// Generate the code to print a trace msg with one optional argument (%T)
  void AddTrace(const std::string &msg, llvm::Value *value = nullptr);

  std::unique_ptr<Engine> engine_;
  std::vector<CompiledExpr *> compiled_exprs_;
  LLVMTypes types_;
  FunctionRegistry function_registry_;
  Annotator annotator_;

  // used in replay/debug
  bool in_replay_;
  bool optimise_ir_;
  bool enable_ir_traces_;
  std::vector<std::string> trace_strings_;
};

} // namespace gandiva

#endif // GANDIVA_LLVMGENERATOR_H
