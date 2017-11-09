<?php

namespace App;

use App\Parsers\PdfParser;
use Illuminate\Contracts\Queue\EntityNotFoundException;
use Illuminate\Foundation\Auth\User as Authenticatable;
use Illuminate\Database\Eloquent\SoftDeletes;
use App\History;
use Illuminate\Support\Facades\DB;
use Illuminate\Http\UploadedFile;
use \Smalot\PdfParser\Parser;
use \PhpOffice\PhpWord\IOFactory;


class User extends Authenticatable
{
    use SoftDeletes;

    protected $primaryKey = 'id';

    /**
     * The attributes that are mass assignable.
     *
     * @var array
     */

    protected $fillable = [
        'first_name',
        'last_name',
        'patronymic',
        'birthday',
        'position_id',
        'salary',
        'currency_id',
        'residence_id',
        'cv_src',
        'location_id',
        'relocatable',
        'education',
        'lang',
        'email',
        'phone',
        'skype',
        'user_links',
        'company_id',
        'linkedin',
        'facebook',
        'vk',
        'google_plus',
        'notes',
        'about',
        'img_name',
        'cv_name',
    ];

    private $img_file;
    private $cv_file;

    public static $dictionaryFeilds = [
        'position',
        'currency',
        'residence',
        'location',
    ];

    /**
     * The attributes that should be hidden for arrays.
     *
     * @var array
     */
    protected $hidden = [
        'password', 'remember_token',
    ];

    /**
     * @param $field
     * @return null
     */
    public function dictionaryValue($field)
    {
        if (!in_array($field, self::$dictionaryFeilds)) {
            throw new EntityNotFoundException('Field ".$field." is not dictionary');
        }
        $model = $this->belongsTo(Dictionary::class, null, null, $field)->first();
        if (empty($model)) {
            return null;
        }
        return $model->value;
    }

    /**
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function company()
    {
        return $this->belongsTo(Company::class);
    }

    /**
     * @param bool $class
     * @return bool|float|string
     */
    public function getCompleteness($class = false)
    {
        $filled = 0;
        foreach ($this->fillable as $val) {
            $filled = !empty($this->$val) ? ++$filled : $filled;
        }
        $filled = round($filled / count($this->fillable) * 100, 1);

        if ($class) {
            if ($filled < 60) {
                $class = 'danger';
            } else if ($filled < 90) {
                $class = 'info';
            } else {
                $class = 'success';
            }
        }
        return $class ? $class : $filled;
    }

    /**
     *
     */
    public function proceedFileName()
    {
        if ($this->img_name != null && $this->img_name instanceof UploadedFile) {
            $this->img_file = $this->img_name;
            $this->img_name = $this->img_name->getClientOriginalName();
        }

        if ($this->cv_name != null && $this->img_name instanceof UploadedFile) {
            $this->cv_file = $this->cv_name;
            $this->cv_name = $this->cv_name->getClientOriginalName();
        }
    }

    /**
     *
     */
    public function saveFiles()
    {
        $umask = umask(0);
        if ($this->img_file != null) {
            $img_path = 'uploads/img/' . $this->id;

            if (file_exists($img_path)) {
                unlink($img_path);
            }

            $this->img_file->move(public_path() . '/uploads/img', $this->id);
            chmod($img_path, 0666);
        }

        if ($this->cv_file != null) {
            $cv_path = 'uploads/cvs/' . $this->id;

            if (file_exists($cv_path)) {
                unlink($cv_path);
            }

            $this->cv_file->move(public_path() . '/uploads/cvs', $this->id);
            chmod($cv_path, 0666);
        }
        umask($umask);
    }

    /**
     *
     */
    public function deleteFiles()
    {
        $img_path = 'uploads/img/' . $this->id;

        if (!empty($this->img_name) && file_exists($img_path)) {
            unlink($img_path);
        }

        $cv_path = 'uploads/cvs/' . $this->id;
        if (!empty($this->cv_name) && file_exists($cv_path)) {
            unlink($cv_path);
        }
    }

    /**
     * @param bool $update
     * @return array|null
     */
    public static function getRules($id = false)
    {
        static $rules = null;
        if (empty($rules)) {
            $rules = [
                'first_name' => 'required|max:25|min:4',
                'last_name' => 'required|max:25|min:4',
                'patronymic' => 'max:25|min:4',
                'birthday' => 'date|after:-70 years|before:-18 years',
                'position_id' => 'exists:dictionaries,id|numeric',
                'currency_id' => 'exists:dictionaries,id|numeric',
                'residence_id' => 'exists:dictionaries,id|numeric',
                'cv_src' => 'required',
                'location_id' => 'exists:dictionaries,id|numeric',
                'user_links' => '',
                'company_id' => 'exists:companies,id',
                'linkedin' => 'url',
                'facebook' => 'url',
                'vk' => 'url',
                'google_plus' => 'url',
            ];
        }

        $rules['email'] = $id ? 'required|email|unique:users,email,' . $id : 'required|email|unique:users,email';
        $rules['skype'] = $id ? 'unique:users,skype,' . $id : 'unique:users,skype';
        $rules['phone'] = $id ? 'unique:users,phone,' . $id : 'unique:users,phone';
        return $rules;
    }

    /**
     * @return \Illuminate\Database\Eloquent\Relations\HasMany
     */
    public function candidate()
    {
        return $this->hasMany(Candidate::class);
    }

    /**
     * @return mixed
     */
    public function history()
    {
        return $this->hasMany(History::class)->orderBy('created_at', 'desc');
    }

    /**
     * @param $permission
     * @return bool
     */
    public function allowed($permission)
    {
        return boolval(DB::table('permissions')->where('role', $this->role)->where('permission', $permission)->count());
    }

    /**
     * @param UploadedFile $file
     * @return User
     */
    public static function fromFile(UploadedFile $file)
    {
        if ($file != null) {
            return app()->make($file->extension())->parse($file);
        }
    }
}