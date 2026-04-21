import { useState, useEffect, useCallback, useId } from 'react';
import { useNavigate } from 'react-router-dom';
import { PageLayout } from '../components/common/PageLayout';
import { PageHeader } from '../components/common/PageHeader';
import { FormInput } from '../components/common/FormInput';
import { Button } from '../components/common/Button';
import { Alert } from '../components/common/Alert';
import { Modal } from '../components/common/Modal';
import { Plus, Edit, Trash2 } from 'lucide-react';
import { BASE_PATH, joinBasePath } from '../utils/basePath';

interface User {
  username: string;
  email?: string;
  roles: string[];
  disabled?: boolean;
  created_at?: string;
  last_login?: string;
  auth_method?: string;
}

export function AdminUsers() {
  const navigate = useNavigate();
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [isAdmin, setIsAdmin] = useState(false);
  const [availableRoles, setAvailableRoles] = useState<string[]>(['admin', 'editor', 'viewer']);
  const createUsernameId = useId();
  const createPasswordId = useId();
  const createEmailId = useId();
  const createRolesId = useId();
  const editRolesId = useId();

  // Create user state
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newUsername, setNewUsername] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newEmail, setNewEmail] = useState('');
  const [newRoles, setNewRoles] = useState<string[]>(['viewer']);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState('');
  
  // Edit user state
  const [editingUser, setEditingUser] = useState<User | null>(null);
  const [editRoles, setEditRoles] = useState<string[]>([]);
  const [editDisabled, setEditDisabled] = useState(false);
  const [updating, setUpdating] = useState(false);
  const [updateError, setUpdateError] = useState('');

  const loadUsers = useCallback(async () => {
    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/users'), {
        credentials: 'include'
      });
      
      if (!response.ok) {
        throw new Error('Failed to load users');
      }
      
      const data = await response.json();
      setUsers(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load users');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadRoles = useCallback(async () => {
    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/roles'), { credentials: 'include' });
      if (response.ok) {
        const names = await response.json();
        if (Array.isArray(names) && names.length > 0) {
          setAvailableRoles(names);
        }
      }
    } catch {
      // Keep default ['admin', 'editor', 'viewer'] if API unavailable
    }
  }, []);

  useEffect(() => {
    // Check if user is admin
    fetch(joinBasePath(BASE_PATH, '/auth/me'), {
      credentials: 'include'
    })
      .then(res => res.json())
      .then(data => {
        const roles = data.roles || [];
        if (!roles.includes('admin')) {
          navigate('/security');
          return;
        }
        setIsAdmin(true);
        loadUsers();
        loadRoles();
      })
      .catch(() => {
        navigate('/security');
      });
  }, [navigate, loadUsers, loadRoles]);

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreateError('');
    setCreating(true);

    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/users'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          username: newUsername,
          password: newPassword,
          roles: newRoles,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to create user');
      }

      // Reset form
      setNewUsername('');
      setNewPassword('');
      setNewEmail('');
      setNewRoles(['viewer']);
      setShowCreateForm(false);
      
      // Reload users
      await loadUsers();
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : 'Failed to create user');
    } finally {
      setCreating(false);
    }
  };

  const handleEditUser = (user: User) => {
    setEditingUser(user);
    setEditRoles([...user.roles]);
    setEditDisabled(user.disabled || false);
    setUpdateError('');
  };

  const handleUpdateUser = async () => {
    if (!editingUser) return;
    
    setUpdateError('');
    setUpdating(true);

    try {
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/users/${editingUser.username}`), {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          roles: editRoles,
          disabled: editDisabled,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to update user');
      }

      setEditingUser(null);
      await loadUsers();
    } catch (err) {
      setUpdateError(err instanceof Error ? err.message : 'Failed to update user');
    } finally {
      setUpdating(false);
    }
  };

  const handleDeleteUser = async (username: string) => {
    if (!confirm(`Are you sure you want to delete user "${username}"? This action cannot be undone.`)) {
      return;
    }

    try {
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/users/${username}`), {
        method: 'DELETE',
        credentials: 'include',
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to delete user');
      }

      await loadUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete user');
    }
  };

  const toggleRole = (role: string, roles: string[], setRoles: (roles: string[]) => void) => {
    if (roles.includes(role)) {
      setRoles(roles.filter(r => r !== role));
    } else {
      setRoles([...roles, role]);
    }
  };

  if (!isAdmin) {
    return null;
  }

  if (loading) {
    return (
      <PageLayout>
        <div className="flex items-center justify-center flex-1">
          <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
        </div>
      </PageLayout>
    );
  }

  return (
    <PageLayout>
      <PageHeader
        title="User Management"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <Button
            variant={showCreateForm ? "secondary" : "primary"}
            onClick={() => setShowCreateForm(!showCreateForm)}
            icon={showCreateForm ? undefined : Plus}
          >
            {showCreateForm ? 'Cancel' : 'Create User'}
          </Button>
        }
      />

      {/* Main Content */}
      <main className="max-w-6xl mx-auto p-6">
        {error && <Alert type="error" message={error} className="mb-6" dismissible onDismiss={() => setError('')} />}

        {/* Create User Form */}
        {showCreateForm && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-6">
            <h2 className="text-lg font-semibold text-white mb-4">Create New User</h2>
            <form onSubmit={handleCreateUser} className="space-y-4">
              <FormInput
                id={createUsernameId}
                label="Username"
                value={newUsername}
                onChange={setNewUsername}
                required
              />

              <FormInput
                id={createPasswordId}
                type="password"
                label="Password"
                value={newPassword}
                onChange={setNewPassword}
                required
              />
              <p className="text-xs text-norse-fog -mt-2">Minimum 8 characters</p>

              <FormInput
                id={createEmailId}
                type="email"
                label="Email (optional)"
                value={newEmail}
                onChange={setNewEmail}
              />

              <fieldset>
                <legend id={createRolesId} className="text-sm text-norse-silver mb-2">Roles *</legend>
                <div className="flex gap-4 flex-wrap">
                  {availableRoles.map(role => (
                    <label key={role} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={newRoles.includes(role)}
                        onChange={() => toggleRole(role, newRoles, setNewRoles)}
                        className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary focus:ring-nornic-primary"
                        aria-describedby={createRolesId}
                      />
                      <span className="text-sm capitalize text-norse-silver">{role}</span>
                    </label>
                  ))}
                </div>
              </fieldset>

              {createError && <Alert type="error" message={createError} />}

              <Button
                type="submit"
                disabled={creating || newRoles.length === 0}
                loading={creating}
                variant="success"
              >
                Create User
              </Button>
            </form>
          </div>
        )}

        {/* Users Table */}
        <div className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-norse-stone">
                <tr>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Username</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Email</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Roles</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Status</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Last Login</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold text-norse-silver">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-norse-rune">
                {users.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-norse-fog">
                      No users found
                    </td>
                  </tr>
                ) : (
                  users.map((user) => (
                    <tr key={user.username} className="hover:bg-norse-stone/50">
                      <td className="px-4 py-3">
                        <div className="font-medium text-white">{user.username}</div>
                      </td>
                      <td className="px-4 py-3 text-norse-silver">
                        {user.email || <span className="text-norse-fog">—</span>}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex gap-1 flex-wrap">
                          {user.roles.map(role => (
                            <span
                              key={role}
                              className={`px-2 py-1 rounded text-xs ${
                                role === 'admin'
                                  ? 'bg-red-500/20 text-red-400'
                                  : role === 'editor'
                                  ? 'bg-blue-500/20 text-blue-400'
                                  : 'bg-norse-stone text-norse-silver'
                              }`}
                            >
                              {role}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        {user.disabled ? (
                          <span className="px-2 py-1 rounded text-xs bg-red-500/20 text-red-400">
                            Disabled
                          </span>
                        ) : (
                          <span className="px-2 py-1 rounded text-xs bg-green-500/20 text-green-400">
                            Active
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-norse-fog text-sm">
                        {user.last_login
                          ? new Date(user.last_login).toLocaleString()
                          : <span className="text-norse-fog">Never</span>}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex gap-2">
                          <Button
                            variant="secondary"
                            size="sm"
                            onClick={() => handleEditUser(user)}
                            icon={Edit}
                          >
                            Edit
                          </Button>
                          <Button
                            variant="danger"
                            size="sm"
                            onClick={() => handleDeleteUser(user.username)}
                            icon={Trash2}
                          >
                            Delete
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Edit User Modal */}
        {editingUser && (
          <Modal
            isOpen={!!editingUser}
            onClose={() => setEditingUser(null)}
            title={`Edit User: ${editingUser.username}`}
          >
            <div className="space-y-4">
              <fieldset>
                <legend id={editRolesId} className="text-sm text-norse-silver mb-2">Roles</legend>
                <div className="flex gap-4 flex-wrap">
                  {availableRoles.map(role => (
                    <label key={role} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editRoles.includes(role)}
                        onChange={() => toggleRole(role, editRoles, setEditRoles)}
                        className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary focus:ring-nornic-primary"
                        aria-describedby={editRolesId}
                      />
                      <span className="text-sm capitalize text-norse-silver">{role}</span>
                    </label>
                  ))}
                </div>
              </fieldset>

              <div>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={editDisabled}
                    onChange={(e) => setEditDisabled(e.target.checked)}
                    className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary focus:ring-nornic-primary"
                  />
                  <span className="text-sm text-norse-silver">Disabled</span>
                </label>
              </div>

              {updateError && <Alert type="error" message={updateError} />}

              <div className="flex gap-2 justify-end pt-4 border-t border-norse-rune">
                <Button
                  variant="secondary"
                  onClick={() => setEditingUser(null)}
                >
                  Cancel
                </Button>
                <Button
                  onClick={handleUpdateUser}
                  disabled={updating || editRoles.length === 0}
                  loading={updating}
                >
                  Update
                </Button>
              </div>
            </div>
          </Modal>
        )}
      </main>
    </PageLayout>
  );
}
